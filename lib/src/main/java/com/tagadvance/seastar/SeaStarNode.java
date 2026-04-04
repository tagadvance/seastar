package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import java.util.Map;
import java.util.Optional;

public interface SeaStarNode extends Metadata {

	SeaStarDriverContext context();

	Optional<SeaStarKeyspace> getSeaStarKeyspace(CqlIdentifier id);

	default SeaStarKeyspace newSeaStarKeyspace(final CqlIdentifier id) {
		final var keyspace = new VolatileKeyspace(context(), id);
		putSeaStarKeyspace(id, keyspace);

		return keyspace;
	}

	void putSeaStarKeyspace(CqlIdentifier id, SeaStarKeyspace keyspace);

	void removeSeaStarKeyspace(CqlIdentifier id);

	Map<CqlIdentifier, SeaStarKeyspace> getSeaStarKeyspaces();

}
