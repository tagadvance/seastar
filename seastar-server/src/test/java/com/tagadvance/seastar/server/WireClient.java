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

	private final Socket socket;

	private boolean segmented;

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
	 * The bytes one request becomes on a v5 connection: a self-contained segment carrying a CRC24
	 * over its header and a CRC32 over its payload. Exposed so that a test can corrupt them.
	 *
	 * @param protocolVersion the version to put in the frame header
	 * @param streamId        the stream id to send it on
	 * @param request         the message to frame
	 * @return the segment, header and trailing checksum included
	 */
	byte[] encodeSegment(final int protocolVersion, final int streamId, final Message request) {
		return segment(
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
			writeRaw(segment(encoded));

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
	 * Wraps one encoded frame in a self-contained segment. The frame buffer is consumed: with no
	 * compression the codec passes it straight through as the payload.
	 */
	private byte[] segment(final ByteBuf frame) {
		final var parts = new ArrayList<>();
		segmentCodec.encode(new Segment<>(frame, true), parts);

		final var out = new ByteArrayOutputStream();
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

		return out.toByteArray();
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
	 * Reads one whole segment and decodes every frame in it. Only self-contained segments are
	 * handled: nothing this server answers with is anywhere near the 128 KiB a split would need.
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
		try {
			do {
				pending.add(codec.decode(payload));
			} while (payload.isReadable());
		} finally {
			payload.release();
		}
	}

	@Override
	public void close() throws IOException {
		socket.close();
	}
}
