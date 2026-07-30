package com.tagadvance.seastar.server;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.CrcMismatchException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.Segment;
import com.datastax.oss.protocol.internal.SegmentCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

/**
 * A blocking native-protocol client built from the driver's own codecs, for tests that need to say
 * exactly what goes on the wire and see exactly what comes back.
 *
 * <p>{@link FrameCodec#defaultClient} is the mirror of the {@code defaultServer} the listener uses
 * - it encodes requests and decodes responses - so a round trip through this class exercises the
 * real framing in both directions without a {@code CqlSession} in the way. The same goes for
 * protocol v5's segments: {@link SegmentCodec} is the one the driver's own handlers delegate to,
 * driven here by hand so that a test can say exactly which bytes go out - including wrong ones.
 */
final class WireClient implements AutoCloseable {

	private final ByteBufPrimitiveCodec primitiveCodec = new ByteBufPrimitiveCodec(
		UnpooledByteBufAllocator.DEFAULT);

	private final FrameCodec<ByteBuf> codec = FrameCodec.defaultClient(primitiveCodec,
		Compressor.none());

	private final SegmentCodec<ByteBuf> segmentCodec = new SegmentCodec<>(primitiveCodec,
		Compressor.none());

	/**
	 * Frames decoded out of a segment but not yet handed back, since one self-contained segment may
	 * carry several.
	 */
	private final Deque<Frame> pending = new ArrayDeque<>();

	/**
	 * The slices of a frame too large for one segment, accumulated until there are enough of them to
	 * decode it.
	 */
	private final ByteArrayOutputStream accumulated = new ByteArrayOutputStream();

	/** The length the accumulated slices add up to when the frame is whole, or 0 between frames. */
	private int accumulatedTarget;

	private final Socket socket;

	private boolean segmented;

	private int segmentsRead;

	private int slicesRead;

	private int slicesWritten;

	WireClient(final int port) throws IOException {
		socket = new Socket(InetAddress.getLoopbackAddress(), port);
		socket.setSoTimeout(10_000);
	}

	/**
	 * Switches this client to protocol v5's segment framing, which is what the listener does to its
	 * own pipeline once it has written the {@code READY}. Call it straight after a v5 {@code STARTUP}
	 * has been answered, and never before.
	 */
	void segments() {
		segmented = true;
	}

	/**
	 * @return how many segments have been read so far, which is more than the number of responses
	 *     whenever one of them had to be split
	 */
	int segmentsRead() {
		return segmentsRead;
	}

	/**
	 * @return how many of the segments read so far were slices of a frame too large to fit in one,
	 *     rather than self-contained segments
	 */
	int slicesRead() {
		return slicesRead;
	}

	/**
	 * @return how many slices of a frame too large for one segment have been sent, so that a test can
	 *     say the request really was split rather than assume it from its size
	 */
	int slicesWritten() {
		return slicesWritten;
	}

	/**
	 * The bytes one request becomes on a v5 connection: a self-contained segment carrying a CRC24
	 * over its header and a CRC32 over its payload. Exposed so that a test can corrupt them.
	 *
	 * @param protocolVersion the version to put in the frame header
	 * @param streamId        the stream id to send it on
	 * @param request         the message to frame
	 * @return the segment, header and trailing checksum included
	 */
	byte[] encodeSegment(final int protocolVersion, final int streamId, final Message request) {
		return segments(
			codec.encode(Frame.forRequest(protocolVersion, streamId, false, Frame.NO_PAYLOAD,
				request)));
	}

	/**
	 * Writes bytes with no framing of any kind, for a test that has something to say about the bytes
	 * themselves.
	 *
	 * @param bytes what to send
	 * @throws IOException if the socket fails
	 */
	void writeRaw(final byte[] bytes) throws IOException {
		socket.getOutputStream().write(bytes);
		socket.getOutputStream().flush();
	}

	/**
	 * Sends one request and blocks for its response.
	 *
	 * @param protocolVersion the version to put in the header
	 * @param streamId        the stream id to send it on
	 * @param request         the message to send
	 * @return the decoded response
	 * @throws IOException if the socket fails, or the peer hangs up before answering
	 */
	Frame send(final int protocolVersion, final int streamId, final Message request)
		throws IOException {
		return send(protocolVersion, streamId, false, request);
	}

	/**
	 * Sends one request with the tracing flag set as asked, and blocks for its response.
	 *
	 * @param protocolVersion the version to put in the header
	 * @param streamId        the stream id to send it on
	 * @param tracing         whether to set the tracing flag
	 * @param request         the message to send
	 * @return the decoded response
	 * @throws IOException if the socket fails, or the peer hangs up before answering
	 */
	Frame send(final int protocolVersion, final int streamId, final boolean tracing,
		final Message request) throws IOException {
		write(protocolVersion, streamId, tracing, request);

		return read();
	}

	/**
	 * Sends one request and does <em>not</em> wait for it, so that a caller can put several in flight
	 * at once and read the answers afterwards.
	 *
	 * @param protocolVersion the version to put in the header
	 * @param streamId        the stream id to send it on
	 * @param tracing         whether to set the tracing flag
	 * @param request         the message to send
	 * @throws IOException if the socket fails
	 */
	void write(final int protocolVersion, final int streamId, final boolean tracing,
		final Message request) throws IOException {
		final var encoded = codec.encode(
			Frame.forRequest(protocolVersion, streamId, tracing, Frame.NO_PAYLOAD, request));
		if (segmented) {
			writeRaw(segments(encoded));

			return;
		}
		try {
			final var bytes = new byte[encoded.readableBytes()];
			encoded.readBytes(bytes);
			writeRaw(bytes);
		} finally {
			encoded.release();
		}
	}

	/**
	 * Wraps one encoded frame in as many segments as it takes. A frame that fits becomes a single
	 * self-contained segment; one that does not becomes a run of slices of at most
	 * {@link Segment#MAX_PAYLOAD_LENGTH} bytes, none of them self-contained and none of them
	 * decodable on its own. That is what the driver's {@code SegmentBuilder} does, written out here so
	 * that a test can send a split request without a {@code CqlSession}.
	 *
	 * <p>The frame buffer is consumed: with no compression the codec passes it straight through as the
	 * payload.
	 */
	private byte[] segments(final ByteBuf frame) {
		final var out = new ByteArrayOutputStream();
		try {
			if (frame.readableBytes() <= Segment.MAX_PAYLOAD_LENGTH) {
				encode(new Segment<>(frame.retain(), true), out);
			} else {
				while (frame.isReadable()) {
					final var length = Math.min(frame.readableBytes(), Segment.MAX_PAYLOAD_LENGTH);
					encode(new Segment<>(frame.readRetainedSlice(length), false), out);
					slicesWritten++;
				}
			}
		} finally {
			frame.release();
		}

		return out.toByteArray();
	}

	/**
	 * Encodes one segment - header, payload and trailing checksum - and appends it. The payload buffer
	 * is consumed.
	 */
	private void encode(final Segment<ByteBuf> segment, final ByteArrayOutputStream out) {
		final var parts = new ArrayList<>();
		segmentCodec.encode(segment, parts);

		for (final var part : parts) {
			final var buffer = (ByteBuf) part;
			try {
				final var bytes = new byte[buffer.readableBytes()];
				buffer.readBytes(bytes);
				out.writeBytes(bytes);
			} finally {
				buffer.release();
			}
		}
	}

	/**
	 * Writes a bare header with an empty body and blocks for the response, for versions
	 * {@link FrameCodec} has no encoder for and which therefore cannot be built as a
	 * {@link Frame} at all.
	 *
	 * @param protocolVersion the version to put in the header
	 * @param streamId        the stream id to send it on
	 * @param opcode          the opcode to claim
	 * @return the decoded response
	 * @throws IOException if the socket fails, or the peer hangs up before answering
	 */
	Frame sendHeader(final int protocolVersion, final int streamId, final int opcode)
		throws IOException {
		final var header = ByteBuffer.allocate(Protocol.HEADER_LENGTH)
			.put((byte) protocolVersion)
			.put((byte) 0)
			.putShort((short) streamId)
			.put((byte) opcode)
			.putInt(0)
			.array();
		socket.getOutputStream().write(header);
		socket.getOutputStream().flush();

		return read();
	}

	/**
	 * Blocks for one response, whatever stream id it is on.
	 *
	 * @return the decoded response
	 * @throws IOException if the socket fails, or the peer hangs up before answering
	 */
	Frame read() throws IOException {
		if (segmented) {
			while (pending.isEmpty()) {
				readSegment();
			}

			return pending.poll();
		}

		final var in = new DataInputStream(socket.getInputStream());
		final var header = new byte[9];
		in.readFully(header);
		final var body = new byte[ByteBuffer.wrap(header, 5, 4).getInt()];
		in.readFully(body);

		final var buffer = Unpooled.wrappedBuffer(header, body);
		try {
			return codec.decode(buffer);
		} finally {
			buffer.release();
		}
	}

	/**
	 * Reads one whole segment. A self-contained one yields every frame in it; a slice of a larger
	 * frame yields nothing until the last slice arrives, at which point the frame is reassembled and
	 * decoded - the same two cases, and the same read-ahead for the target length, that the driver's
	 * {@code SegmentToFrameDecoder} handles.
	 */
	private void readSegment() throws IOException {
		final var in = new DataInputStream(socket.getInputStream());
		final var headerBytes = new byte[segmentCodec.headerLength() + SegmentCodec.CRC24_LENGTH];
		in.readFully(headerBytes);

		final SegmentCodec.Header header;
		final var headerBuffer = Unpooled.wrappedBuffer(headerBytes);
		try {
			header = segmentCodec.decodeHeader(headerBuffer);
		} catch (final CrcMismatchException e) {
			throw new IllegalStateException("the listener sent a segment that fails its own CRC", e);
		} finally {
			headerBuffer.release();
		}

		final var body = new byte[header.payloadLength + SegmentCodec.CRC32_LENGTH];
		in.readFully(body);
		final ByteBuf payload;
		try {
			// decode releases the source and hands back a retained slice of the payload.
			payload = segmentCodec.decode(header, Unpooled.wrappedBuffer(body)).payload;
		} catch (final CrcMismatchException e) {
			throw new IllegalStateException("the listener sent a segment that fails its own CRC", e);
		}
		segmentsRead++;
		try {
			if (header.isSelfContained) {
				do {
					pending.add(codec.decode(payload));
				} while (payload.isReadable());

				return;
			}
			slicesRead++;
			accumulate(payload);
		} finally {
			payload.release();
		}
	}

	/**
	 * Adds one slice of a split frame, and decodes the frame once the slices add up to the length its
	 * header declared. The length is read out of the first slice, which is the only one that carries a
	 * frame header - and is why nothing can be decoded until the last slice has arrived.
	 */
	private void accumulate(final ByteBuf slice) {
		final var bytes = new byte[slice.readableBytes()];
		slice.readBytes(bytes);
		accumulated.writeBytes(bytes);
		if (accumulatedTarget == 0) {
			accumulatedTarget = Protocol.HEADER_LENGTH + ByteBuffer.wrap(bytes, 5, 4).getInt();
		}
		if (accumulated.size() < accumulatedTarget) {
			return;
		}

		final var frame = Unpooled.wrappedBuffer(accumulated.toByteArray());
		try {
			pending.add(codec.decode(frame));
		} finally {
			frame.release();
			accumulated.reset();
			accumulatedTarget = 0;
		}
	}

	@Override
	public void close() throws IOException {
		socket.close();
	}
}
