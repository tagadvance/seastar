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
	 * The versions this server speaks. v3 predates several things the core relies on, and v6 is a
	 * beta a driver only reaches for when it is told to.
	 */
	static final int LOWEST = ProtocolConstants.Version.V4;
	static final int HIGHEST = ProtocolConstants.Version.V5;

	/** What {@code SUPPORTED} advertises, in the shape a node writes it. */
	static final List<String> VERSIONS = List.of("4/v4", "5/v5");

	/** version, flags, stream id, opcode, body length. Unchanged across v3 to v6. */
	static final int HEADER_LENGTH = FrameCodec.V3_ENCODED_HEADER_SIZE;

	private Protocol() {

	}

	/**
	 * @param version the version byte a connection opened with
	 * @return whether this server serves it
	 */
	static boolean speaks(final int version) {
		return version >= LOWEST && version <= HIGHEST;
	}

	/**
	 * Whether everything after the handshake is wrapped in CRC-checked segments. The
	 * {@code OPTIONS}/{@code STARTUP} exchange is in the legacy format at every version; modern
	 * framing begins with the message after {@code READY}.
	 *
	 * @param version the version this connection opened with
	 * @return whether this connection uses segments
	 */
	static boolean isSegmented(final int version) {
		return version >= ProtocolConstants.Version.V5;
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
			"Invalid or unsupported protocol version (" + version + "); supported versions are ("
				+ String.join(", ", VERSIONS) + ")");
	}

	/**
	 * Writes one response, on the stream id of the request it answers.
	 *
	 * <p>No tracing id is ever fabricated, no warning is ever raised, and custom payloads are read
	 * by nothing here, so all three are empty on every response (b_plan B7).
	 *
	 * @param ctx      the context to write from
	 * @param version  the protocol version to write the header with, which is the version of the
	 *                 request being answered
	 * @param streamId the stream id of the request being answered
	 * @param message  the message to send
	 * @return the write's future, so that a caller can hang up once it has flushed
	 */
	static ChannelFuture write(final ChannelHandlerContext ctx, final int version,
		final int streamId, final Message message) {
		return ctx.writeAndFlush(
			Frame.forResponse(version, streamId, null, Frame.NO_PAYLOAD, List.of(), message));
	}
}
