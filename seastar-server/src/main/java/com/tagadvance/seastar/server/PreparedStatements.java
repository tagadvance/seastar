package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;

/**
 * The statements this server has prepared, by the id it handed out for them.
 *
 * <p>One registry per server rather than per connection, because a prepared statement belongs to
 * the node on a real cluster: a driver prepares on one connection and executes on whichever one the
 * pool gives it, and an id that only worked on the connection it was made on would fail
 * intermittently under a pool of four.
 *
 * <p>Nothing is ever evicted. A real node has a bounded cache and answers an id it has forgotten
 * with {@code UNPREPARED} so the driver re-prepares; here the session is the process and lives as
 * long as the test does, so an entry costs one map slot and a forgotten id would only invent a
 * failure mode nobody asked for.
 */
@ThreadSafe
final class PreparedStatements {

	// Keyed by the id's bytes rather than by the buffer the driver hands out: ByteBuffer equality is
	// over the readable region, so a buffer somebody has read from stops matching itself.
	private final Map<ByteBuffer, PreparedStatement> byId = new ConcurrentHashMap<>();

	/**
	 * @param statement the statement to remember
	 * @return the id to send back, as the protocol carries it
	 */
	byte[] register(final PreparedStatement statement) {
		final var id = Bytes.getArray(statement.getId());
		byId.put(ByteBuffer.wrap(id), statement);

		return id;
	}

	/**
	 * @param id the id from an {@code EXECUTE} or a {@code BATCH}
	 * @return the statement it names, or empty if this server never handed that id out
	 */
	Optional<PreparedStatement> find(final byte[] id) {
		return Optional.ofNullable(byId.get(ByteBuffer.wrap(id)));
	}

}
