package com.tagadvance.seastar.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;

/**
 * What this server says about protocol versions, and the single way it puts a message on the wire.
 */
final class Protocol {

	/**
	 * The one version this server speaks. v5 wraps everything after the handshake in CRC-checked
	 * segments, which is deferred (f_plan F1), and v3 predates several things the core relies on.
	 */
	static final int VERSION = ProtocolConstants.Version.V4;

	/** version, flags, stream id, opcode, body length. Unchanged across v3 to v6. */
	static final int HEADER_LENGTH = FrameCodec.V3_ENCODED_HEADER_SIZE;

	private Protocol() {

	}

	/**
	 * The refusal that makes a driver step down a version instead of giving up.
	 *
	 * <p>The wording is load-bearing. {@code ProtocolInitHandler} looks for a
	 * {@code PROTOCOL_ERROR} or {@code SERVER_ERROR} on the first request of a channel whose
	 * message <em>contains</em> {@code "Invalid or unsupported protocol version"}, and only then
	 * does {@code ChannelFactory} retry one version lower. Paraphrase it and a driver that was
	 * not configured with a version fails outright - or, worse, hangs with nothing in the log to
	 * explain why.
	 *
	 * <p>The surrounding shape is what cassandra:5.0.8 sends, captured rather than guessed: the
	 * error carries the version the <em>server</em> speaks in its header, not the one that was
	 * asked for.
	 *
	 * @param version the version that was asked for
	 * @return the error to send back
	 */
	static Error unsupportedVersion(final int version) {
		return new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
			"Invalid or unsupported protocol version (" + version
				+ "); supported versions are (4/v4)");
	}

	/**
	 * Writes one response, on the stream id of the request it answers.
	 *
	 * <p>No tracing id is ever fabricated, no warning is ever raised, and custom payloads are read
	 * by nothing here, so all three are empty on every response (b_plan B7).
	 *
	 * @param ctx      the context to write from
	 * @param streamId the stream id of the request being answered
	 * @param message  the message to send
	 * @return the write's future, so that a caller can hang up once it has flushed
	 */
	static ChannelFuture write(final ChannelHandlerContext ctx, final int streamId,
		final Message message) {
		return ctx.writeAndFlush(
			Frame.forResponse(VERSION, streamId, null, Frame.NO_PAYLOAD, List.of(), message));
	}
}
