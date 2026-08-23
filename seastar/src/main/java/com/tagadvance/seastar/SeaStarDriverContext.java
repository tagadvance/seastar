package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;

/**
 * The root of SeaStar's storage model and the driver's {@link DriverContext}/{@link Metadata} at
 * once - one object serves both roles, so there is no sync problem between the two. Reachable via
 * {@code SeaStarCqlSession#getContext()}; a test typically reaches for
 * {@link #newSeaStarKeyspace(String)} to populate data directly rather than through CQL. Guards
 * only the keyspace map itself; see the lock hierarchy in {@code AGENTS.md}.
 */
public interface SeaStarDriverContext extends SeaStarReadWriteLock, DriverContext, Metadata {

	/**
	 * The single node this session reports - the same instance for the session's lifetime, and the
	 * only entry in {@link #getNodes()}.
	 */
	Node getNode();

	/**
	 * The clock a write is stamped with and a TTL expires against. Defaults to
	 * {@link Clock#systemUTC()}; a session built with {@link SeaStarCqlSessionBuilder#withClock} gets
	 * that one instead, which is how a test observes expiry without waiting for it.
	 */
	Clock getClock();

	/**
	 * Shortcut for {@link #getSeaStarKeyspace(CqlIdentifier)} with
	 * {@link CqlIdentifier#fromInternal(String)}: the name is case-sensitive and unquoted, not the
	 * {@code fromCql} rules {@link Metadata#getKeyspace(String)} applies.
	 */
	default Optional<SeaStarKeyspace> getSeaStarKeyspace(final String name) {
		return getSeaStarKeyspace(CqlIdentifier.fromInternal(name));
	}

	/**
	 * Looks up a keyspace under the context read lock. The result is the live keyspace, guarded by
	 * its own lock - not a copy.
	 */
	Optional<SeaStarKeyspace> getSeaStarKeyspace(CqlIdentifier id);

	/**
	 * The replication a keyspace gets when it is created outside CQL, matching what
	 * {@code CREATE KEYSPACE ... WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':
	 * 1}} produces on a live cluster.
	 */
	Map<String, String> DEFAULT_REPLICATION = Map.of("class",
		"org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1");

	/**
	 * Cassandra's own default for {@code durable_writes}.
	 */
	boolean DEFAULT_DURABLE_WRITES = true;

	/**
	 * Shortcut for {@link #newSeaStarKeyspace(CqlIdentifier)} with
	 * {@link CqlIdentifier#fromInternal(String)}, so the name is case-sensitive and unquoted.
	 */
	default SeaStarKeyspace newSeaStarKeyspace(final String name) {
		return newSeaStarKeyspace(CqlIdentifier.fromInternal(name));
	}

	/**
	 * Creates a keyspace with {@link #DEFAULT_REPLICATION} and {@link #DEFAULT_DURABLE_WRITES}.
	 */
	default SeaStarKeyspace newSeaStarKeyspace(final CqlIdentifier id) {
		return newSeaStarKeyspace(id, DEFAULT_REPLICATION, DEFAULT_DURABLE_WRITES);
	}

	/**
	 * Creates a keyspace and registers it, bypassing CQL - how a test seeds data directly. Unlike
	 * {@code CREATE KEYSPACE} there is no already-exists check: a keyspace registered under the same
	 * id is replaced silently, along with everything in it.
	 */
	default SeaStarKeyspace newSeaStarKeyspace(final CqlIdentifier id,
		final Map<String, String> replication, final boolean durableWrites) {
		final var keyspace = new VolatileKeyspace(this, id, replication, durableWrites);
		putSeaStarKeyspace(keyspace);

		return keyspace;
	}

	/**
	 * Registers a keyspace, taking the context write lock. An existing keyspace under the same name
	 * is replaced silently - {@code CREATE KEYSPACE}'s already-exists check lives in its handler,
	 * not here.
	 */
	void putSeaStarKeyspace(SeaStarKeyspace keyspace);

	/**
	 * Deregisters a keyspace, taking the context write lock. An id that is not registered is a
	 * no-op.
	 */
	void removeSeaStarKeyspace(CqlIdentifier id);

	/**
	 * A snapshot taken under the context read lock: the map is a copy, safe to iterate against
	 * concurrent DDL, but the keyspaces in it are the live objects.
	 */
	Map<CqlIdentifier, SeaStarKeyspace> getSeaStarKeyspaces();

}
