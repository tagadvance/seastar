package com.tagadvance.seastar.server;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.connection.CrcMismatchException;
import com.datastax.oss.driver.internal.core.protocol.FrameDecodingException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatch loop: one decoded {@link Frame} in, one {@link Frame} out, on the request's own
 * stream id. One instance per connection.
 *
 * <p>Every frame is answered on the server's funnel rather than on the Netty event loop that
 * decoded it, which is what keeps SeaStar's "no interleaving between one session's requests"
 * invariant true over the wire. Responses may therefore complete out of the order
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
	 * What {@code OPTIONS} is answered with. {@code CQL_VERSION} is the value cassandra:5.0.8
	 * reports, since that is the release whose parser SeaStar borrows. {@code COMPRESSION} is
	 * deliberately empty - see {@link #startup(Startup)}.
	 */
	private static final Map<String, List<String>> SUPPORTED_OPTIONS = Map.of(
		Startup.CQL_VERSION_KEY, List.of("3.4.7"),
		Startup.COMPRESSION_KEY, List.of(),
		"PROTOCOL_VERSIONS", Protocol.VERSIONS);

	/**
	 * The event types a {@code REGISTER} may name, in the case they are written on the wire. A node
	 * rejects anything else rather than accepting a subscription it will never honour, and so does
	 * this - but it resolves the name case-insensitively, so the lookup is against the upper-cased
	 * form. See {@link #register(Register)}.
	 */
	private static final Set<String> EVENT_TYPES = Set.of(
		ProtocolConstants.EventType.TOPOLOGY_CHANGE, ProtocolConstants.EventType.STATUS_CHANGE,
		ProtocolConstants.EventType.SCHEMA_CHANGE);

	private final SeaStarRequestDispatcher dispatcher;
	private final Executor funnel;
	private final SeaStarConnection connection;
	private final Collection<SeaStarConnection> connections;
	private final Framing framing;

	SeaStarProtocolHandler(final SeaStarRequestDispatcher dispatcher, final Executor funnel,
		final SeaStarConnection connection, final Collection<SeaStarConnection> connections,
		final Framing framing) {
		this.dispatcher = requireNonNull(dispatcher, "dispatcher must not be null");
		this.funnel = requireNonNull(funnel, "funnel must not be null");
		this.connection = requireNonNull(connection, "connection must not be null");
		this.connections = requireNonNull(connections, "connections must not be null");
		this.framing = requireNonNull(framing, "framing must not be null");
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		// Joining the roll of open connections here rather than at construction, so that a
		// connection is only ever published to once there is a channel able to carry the write.
		connections.add(connection);
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
		connections.remove(connection);
		super.channelInactive(ctx);
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
		// of them. A segment that fails its CRC has no readable stream id at all: the corruption is
		// in the framing rather than in a message, so the answer goes out on stream 0 and the
		// connection ends, which is the only honest thing to do once the byte stream is in doubt.
		final var streamId = cause instanceof FrameDecodingException e ? e.streamId : 0;
		log.debug("closing {} after a frame error", ctx.channel(), cause);
		Protocol.write(ctx, connection.version(), streamId,
				new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, describe(cause)))
			.addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * Names a CRC mismatch for what it is rather than leaving it inside a decoder exception's
	 * {@code toString}, because "the bytes on this connection are not the bytes that were sent" is
	 * a different problem from "this message will not parse" and wants a different investigation.
	 */
	private static String describe(final Throwable cause) {
		final var crcMismatch = crcMismatch(cause);

		return crcMismatch == null ? "Malformed or undecodable frame: " + cause
			: "CRC mismatch on a protocol v5 segment: " + crcMismatch.getMessage();
	}

	private static @Nullable Throwable crcMismatch(final @Nullable Throwable cause) {
		if (cause == null) {
			return null;
		}

		return cause instanceof CrcMismatchException ? cause : crcMismatch(cause.getCause());
	}

	private void respond(final ChannelHandlerContext ctx, final Frame request) {
		Message response;
		try {
			response = answer(request);
		} catch (final RuntimeException e) {
			log.warn("failed to answer {}", request.message, e);
			response = new Error(ProtocolConstants.ErrorCode.SERVER_ERROR, String.valueOf(e));
		}
		if (startsSegmentFraming(request, response)) {
			// One event-loop task carrying both the write and the switch. Enqueued separately from
			// here, the two leave a window: the loop flushes the READY, the client's first segment
			// arrives over loopback and is read through the still-legacy decoder - all before this
			// thread gets to enqueue the switch. On the event loop the READY is encoded inline, in
			// the legacy format, and the pipeline is segmented before the task yields back to I/O,
			// so no read can come between the two.
			final var ready = response;
			ctx.channel().eventLoop().execute(() -> {
				Protocol.write(ctx, request.protocolVersion, request.streamId, ready);
				framing.segmented(ctx.pipeline());
			});
		} else {
			Protocol.write(ctx, request.protocolVersion, request.streamId, response);
		}
	}

	/**
	 * Whether this exchange is the one after which protocol v5 stops using legacy frames. The
	 * driver switches on receiving the {@code READY}; a server has to switch on having sent it.
	 */
	private static boolean startsSegmentFraming(final Frame request, final Message response) {
		return request.message.opcode == ProtocolConstants.Opcode.STARTUP
			&& response instanceof Ready
			&& Protocol.isSegmented(request.protocolVersion);
	}

	private Message answer(final Frame request) {
		if (!Protocol.speaks(request.protocolVersion)) {
			// ProtocolVersionGate has already turned away anything that opened at another version, so
			// this is only reachable if a client changes version mid-connection. It also covers the
			// one client-shaped corner of the driver's FrameDecoder: a v1 or v2 header makes it
			// synthesize an Error *response* and pass it up as though it were a request, and the
			// version check running first is what stops anything from reading that message.
			return Protocol.unsupportedVersion(request.protocolVersion);
		}

		return switch (request.message.opcode) {
			case ProtocolConstants.Opcode.OPTIONS -> new Supported(SUPPORTED_OPTIONS);
			case ProtocolConstants.Opcode.STARTUP -> startup((Startup) request.message);
			case ProtocolConstants.Opcode.REGISTER -> register((Register) request.message);
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

	/**
	 * Records what this connection wants to be told about, and answers {@code READY}.
	 *
	 * <p>Only {@code SCHEMA_CHANGE} is ever published. A single node that is up for as long as the
	 * server is bound has no topology or status change to report, so registering for those two is
	 * accepted and correctly produces nothing.
	 *
	 * <p>Three things here were captured from a {@code cassandra:5.0.8} container rather than
	 * reasoned about, and all three are things a reasonable implementation would get wrong. A node
	 * upper-cases the name before resolving it, so {@code schema_change} registers and is then
	 * honoured - refusing it would leave this server stricter than the thing it imitates. The refusal
	 * quotes the name <em>as it was sent</em>, not the upper-cased form. And one bad name rejects the
	 * whole message: a {@code REGISTER} naming {@code SCHEMA_CHANGE} alongside a type that does not
	 * exist registers nothing at all, which is why nothing is recorded until every name has been
	 * checked.
	 */
	private Message register(final Register request) {
		final var eventTypes = new ArrayList<String>(request.eventTypes.size());
		for (final var eventType : request.eventTypes) {
			final var resolved = eventType.toUpperCase(Locale.ROOT);
			if (!EVENT_TYPES.contains(resolved)) {
				return new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
					"Invalid value '" + eventType + "' for Type");
			}
			eventTypes.add(resolved);
		}
		connection.register(eventTypes);

		return new Ready();
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
}
