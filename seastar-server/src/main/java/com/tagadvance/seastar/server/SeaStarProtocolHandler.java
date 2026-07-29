package com.tagadvance.seastar.server;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.internal.core.protocol.FrameDecodingException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatch loop: one decoded {@link Frame} in, one {@link Frame} out, on the request's own
 * stream id. One instance per connection.
 *
 * <p>Every frame is answered on the server's funnel rather than on the Netty event loop that
 * decoded it, which is what keeps SeaStar's "no interleaving between one session's requests"
 * invariant true over the wire (b_plan B2). Responses may therefore complete out of the order
 * they arrived in; that is legal, and echoing the request's stream id is exactly what makes it
 * legal.
 *
 * <p>Handshake messages are answered here. Statements go to {@link SeaStarRequestDispatcher}.
 *
 * <p>Unusually for a Netty handler this one is touched by two threads on purpose - the event loop
 * decodes, the funnel answers - so it holds no mutable state of its own. What a connection
 * accumulates lives in {@link SeaStarConnection}.
 */
@ThreadSafe
final class SeaStarProtocolHandler extends SimpleChannelInboundHandler<Frame> {

	private static final Logger log = LoggerFactory.getLogger(SeaStarProtocolHandler.class);

	/**
	 * The one protocol version this server speaks. v5 wraps everything after the handshake in
	 * CRC-checked segments, which is deferred (f_plan F1); {@link #unsupportedVersion(int)} is how
	 * a driver on its v5 default still gets here.
	 */
	static final int PROTOCOL_VERSION = ProtocolConstants.Version.V4;

	/**
	 * What {@code OPTIONS} is answered with. {@code CQL_VERSION} is the value cassandra:5.0.8
	 * reports, since that is the release whose parser SeaStar borrows. {@code COMPRESSION} is
	 * deliberately empty - see {@link #startup(Startup)}.
	 */
	private static final Map<String, List<String>> SUPPORTED_OPTIONS = Map.of(
		Startup.CQL_VERSION_KEY, List.of("3.4.7"),
		Startup.COMPRESSION_KEY, List.of(),
		"PROTOCOL_VERSIONS", List.of("4/v4"));

	private final SeaStarRequestDispatcher dispatcher;
	private final Executor funnel;
	private final SeaStarConnection connection = new SeaStarConnection();

	SeaStarProtocolHandler(final SeaStarRequestDispatcher dispatcher, final Executor funnel) {
		this.dispatcher = requireNonNull(dispatcher, "dispatcher must not be null");
		this.funnel = requireNonNull(funnel, "funnel must not be null");
	}

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final Frame request) {
		// Handing the frame to another thread is safe because a decoded request holds no reference
		// to the buffer it came from: ByteBufPrimitiveCodec#readBytes copies, and only the v5+
		// paths use readRetainedSlice.
		try {
			funnel.execute(() -> respond(ctx, request));
		} catch (final RejectedExecutionException e) {
			// The server is closing. The channel is going with it, so there is nobody left to tell.
			log.debug("dropped {} on a closing server", request.message, e);
		}
	}

	@Override
	public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
		// A frame that will not decode leaves the stream unreadable, so answer and hang up. The
		// stream id survives even a failed decode - the driver's FrameDecoder reads it off the
		// header before giving up - which is what lets the client fail one request instead of all
		// of them.
		final var streamId = cause instanceof FrameDecodingException e ? e.streamId : 0;
		log.debug("closing {} after a frame error", ctx.channel(), cause);
		write(ctx, streamId, new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
			"Malformed or undecodable frame: " + cause)).addListener(ChannelFutureListener.CLOSE);
	}

	private void respond(final ChannelHandlerContext ctx, final Frame request) {
		Message response;
		try {
			response = answer(request);
		} catch (final RuntimeException e) {
			log.warn("failed to answer {}", request.message, e);
			response = new Error(ProtocolConstants.ErrorCode.SERVER_ERROR, String.valueOf(e));
		}
		write(ctx, request.streamId, response);
	}

	private Message answer(final Frame request) {
		if (request.protocolVersion != PROTOCOL_VERSION) {
			// This also covers the one client-shaped corner of the driver's FrameDecoder: a v1 or
			// v2 header makes it synthesize an Error *response* and pass it up as though it were a
			// request. The version check runs first, so nothing ever reads that message.
			return unsupportedVersion(request.protocolVersion);
		}

		return switch (request.message.opcode) {
			case ProtocolConstants.Opcode.OPTIONS -> new Supported(SUPPORTED_OPTIONS);
			case ProtocolConstants.Opcode.STARTUP -> startup((Startup) request.message);
			// Events are not published at all yet (f_plan F2), so registering for them records
			// nothing. Answering READY is what a node does and what the driver's init expects.
			case ProtocolConstants.Opcode.REGISTER -> new Ready();
			// Never sent unprompted by this server: replying READY to STARTUP means "no
			// authentication required", which a driver without credentials expects and a driver
			// with them tolerates. Handled anyway, in case one offers a token regardless.
			case ProtocolConstants.Opcode.AUTH_RESPONSE -> new AuthSuccess(null);
			case ProtocolConstants.Opcode.QUERY, ProtocolConstants.Opcode.PREPARE,
				ProtocolConstants.Opcode.EXECUTE, ProtocolConstants.Opcode.BATCH ->
				dispatcher.dispatch(request, connection);
			default -> new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
				"Unsupported request opcode: " + request.message.opcode);
		};
	}

	private static Message startup(final Startup request) {
		final var compression = request.options.get(Startup.COMPRESSION_KEY);
		if (compression != null && !compression.isEmpty()) {
			// Named rather than silently ignored, matching the standard the core's
			// UnsupportedStatements is held to. Compression buys nothing on a loopback socket, and
			// silently sending uncompressed bodies to a client expecting compressed ones would
			// present as corruption.
			return new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, "Unsupported compression: "
				+ compression + ". This server advertises no COMPRESSION in its SUPPORTED options.");
		}

		return new Ready();
	}

	/**
	 * The refusal that makes a driver step down a version instead of giving up.
	 *
	 * <p>The wording is load-bearing. {@code ProtocolInitHandler} looks for a {@code PROTOCOL_ERROR}
	 * or {@code SERVER_ERROR} on the first request of a channel whose message <em>contains</em>
	 * {@code "Invalid or unsupported protocol version"}, and only then does {@code ChannelFactory}
	 * retry one version lower. Paraphrase it and a driver left on its v5 default fails outright -
	 * or, worse, hangs with nothing in the log to explain why.
	 *
	 * <p>The surrounding shape is what cassandra:5.0.8 sends, captured rather than guessed: the
	 * error carries the version the <em>server</em> speaks in its header, not the one that was
	 * asked for.
	 */
	private static Error unsupportedVersion(final int version) {
		return new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
			"Invalid or unsupported protocol version (" + version
				+ "); supported versions are (4/v4)");
	}

	private static ChannelFuture write(final ChannelHandlerContext ctx, final int streamId,
		final Message message) {
		// No tracing id is ever fabricated, no warning is ever raised, and custom payloads are read
		// by nothing here, so all three are empty on every response (b_plan B7).
		return ctx.writeAndFlush(Frame.forResponse(PROTOCOL_VERSION, streamId, null,
			Frame.NO_PAYLOAD, List.of(), message));
	}
}
