package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.util.Map;
import java.util.Optional;

public interface SeaStarDriverContext extends SeaStarReadWriteLock, DriverContext, Metadata {

	default Optional<SeaStarKeyspace> getSeaStarKeyspace(final String name) {
		return getSeaStarKeyspace(CqlIdentifier.fromInternal(name));
	}

	Optional<SeaStarKeyspace> getSeaStarKeyspace(CqlIdentifier id);

	default SeaStarKeyspace newSeaStarKeyspace(final String name) {
		return newSeaStarKeyspace(CqlIdentifier.fromInternal(name));
	}

	default SeaStarKeyspace newSeaStarKeyspace(final CqlIdentifier id) {
		final var keyspace = new VolatileKeyspace(this, id);
		putSeaStarKeyspace(id, keyspace);

		return keyspace;
	}

	void putSeaStarKeyspace(CqlIdentifier id, SeaStarKeyspace keyspace);

	void removeSeaStarKeyspace(CqlIdentifier id);

	Map<CqlIdentifier, SeaStarKeyspace> getSeaStarKeyspaces();

}
