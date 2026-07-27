package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;

public interface SeaStarDriverContext extends SeaStarReadWriteLock, DriverContext, Metadata {

	Node getNode();

	/**
	 * The clock a write is stamped with and a TTL expires against. Defaults to
	 * {@link Clock#systemUTC()}; a session built with {@link SeaStarCqlSessionBuilder#withClock} gets
	 * that one instead, which is how a test observes expiry without waiting for it.
	 */
	Clock getClock();

	default Optional<SeaStarKeyspace> getSeaStarKeyspace(final String name) {
		return getSeaStarKeyspace(CqlIdentifier.fromInternal(name));
	}

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

	default SeaStarKeyspace newSeaStarKeyspace(final String name) {
		return newSeaStarKeyspace(CqlIdentifier.fromInternal(name));
	}

	default SeaStarKeyspace newSeaStarKeyspace(final CqlIdentifier id) {
		return newSeaStarKeyspace(id, DEFAULT_REPLICATION, DEFAULT_DURABLE_WRITES);
	}

	default SeaStarKeyspace newSeaStarKeyspace(final CqlIdentifier id,
		final Map<String, String> replication, final boolean durableWrites) {
		final var keyspace = new VolatileKeyspace(this, id, replication, durableWrites);
		putSeaStarKeyspace(keyspace);

		return keyspace;
	}

	void putSeaStarKeyspace(SeaStarKeyspace keyspace);

	void removeSeaStarKeyspace(CqlIdentifier id);

	Map<CqlIdentifier, SeaStarKeyspace> getSeaStarKeyspaces();

}
