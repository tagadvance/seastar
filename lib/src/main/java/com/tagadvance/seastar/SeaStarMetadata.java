package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.NonNull;

public final class SeaStarMetadata implements Metadata {

	@Override
	@NonNull
	public Map<UUID, Node> getNodes() {
		return Map.of();
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
		// TODO
		return Map.of();
	}

	@Override
	@NonNull
	public Optional<TokenMap> getTokenMap() {
		return Optional.empty();
	}

}
