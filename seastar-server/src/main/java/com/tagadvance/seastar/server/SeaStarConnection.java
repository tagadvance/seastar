package com.tagadvance.seastar.server;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.response.Event;
import io.netty.channel.Channel;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * The state one wire connection carries that the in-process API does not.
 *
 * <p>The keyspace {@code USE} sets is per connection on a real node, and a driver opens several
 * connections to the same node. {@link com.tagadvance.seastar.SeaStarCqlSession} has one
 * session-wide keyspace instead, so the two do not line up: a harness that runs {@code USE ks} on
 * one connection and an {@code INSERT} on another would otherwise fail, and fail intermittently.
 * The keyspace is tracked here and applied inside the funnel immediately before the statement
 * runs, which is safe precisely because the funnel serializes every request (b_plan B2).
 *
 * <p>The event types a {@code REGISTER} asked for are per connection as well, which is what makes
 * a {@code SCHEMA_CHANGE} reach the connections watching for one rather than the connection that
 * happened to run the DDL.
 */
@ThreadSafe
final class SeaStarConnection {

	/**
	 * A message that answers no request travels on a negative stream id, which is what tells a
	 * client to route it to its event handler instead of to a waiting caller.
	 */
	private static final int EVENT_STREAM_ID = -1;

	private final Channel channel;

	// Only ever touched from the funnel, but volatile all the same: a single-thread executor is
	// free to replace its thread if one dies, and the resulting visibility bug would be invisible.
	private volatile @Nullable CqlIdentifier keyspace;

	// Registered from the funnel and read from the funnel, but a concurrent set rather than a
	// plain one: REGISTER may arrive more than once and is cumulative, and a publish walks every
	// connection rather than only the one being registered.
	private final Set<String> events = ConcurrentHashMap.newKeySet();

	SeaStarConnection(final Channel channel) {
		this.channel = requireNonNull(channel, "channel must not be null");
	}

	/**
	 * @return the keyspace {@code USE} set on this connection, or {@code null} if none was
	 *     selected
	 */
	@Nullable
	CqlIdentifier keyspace() {
		return keyspace;
	}

	void keyspace(final @Nullable CqlIdentifier keyspace) {
		this.keyspace = keyspace;
	}

	/**
	 * Records the event types this connection asked to be told about. Cumulative, as the protocol
	 * says: a second {@code REGISTER} adds to the first rather than replacing it.
	 *
	 * @param eventTypes the types to add
	 */
	void register(final Collection<String> eventTypes) {
		events.addAll(eventTypes);
	}

	/**
	 * Pushes an event to this connection, if it registered for that type.
	 *
	 * <p>The funnel decides <em>when</em>; the write is handed to Netty, which runs it on this
	 * channel's own event loop and therefore serializes it against every other write on the
	 * channel. Nothing waits for it: an event answers no request, so there is no caller to fail.
	 *
	 * @param event the event to publish
	 */
	void publish(final Event event) {
		if (!events.contains(event.type)) {
			return;
		}

		channel.writeAndFlush(Frame.forResponse(Protocol.VERSION, EVENT_STREAM_ID, null,
			Frame.NO_PAYLOAD, List.of(), event));
	}
}
