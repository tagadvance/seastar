package com.tagadvance.seastar.server;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * A blocking native-protocol client built from the driver's own codecs, for tests that need to say
 * exactly what goes on the wire and see exactly what comes back.
 *
 * <p>{@link FrameCodec#defaultClient} is the mirror of the {@code defaultServer} the listener uses
 * - it encodes requests and decodes responses - so a round trip through this class exercises the
 * real framing in both directions without a {@code CqlSession} in the way.
 */
final class WireClient implements AutoCloseable {

	private final FrameCodec<ByteBuf> codec = FrameCodec.defaultClient(
		new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT), Compressor.none());

	private final Socket socket;

	WireClient(final int port) throws IOException {
		socket = new Socket(InetAddress.getLoopbackAddress(), port);
		socket.setSoTimeout(10_000);
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
		try {
			final var bytes = new byte[encoded.readableBytes()];
			encoded.readBytes(bytes);
			socket.getOutputStream().write(bytes);
			socket.getOutputStream().flush();
		} finally {
			encoded.release();
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

	@Override
	public void close() throws IOException {
		socket.close();
	}
}
