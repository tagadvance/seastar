package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class VolatileNode implements SeaStarNode {

	private final SeaStarDriverContext context;
	private final Map<CqlIdentifier, SeaStarKeyspace> keyspaceById;

	public VolatileNode(final SeaStarDriverContext context) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspaceById = new ConcurrentHashMap<>();
	}

	@Override
	public SeaStarDriverContext context() {
		return context;
	}

	@Override
	public Optional<SeaStarKeyspace> getSeaStarKeyspace(final CqlIdentifier id) {
		return Optional.of(id).map(keyspaceById::get);
	}

	public void putSeaStarKeyspace(final CqlIdentifier id, final SeaStarKeyspace keyspace) {
		keyspaceById.put(id, keyspace);
	}

	public void removeSeaStarKeyspace(final CqlIdentifier id) {
		keyspaceById.remove(id);
	}

	public Map<CqlIdentifier, SeaStarKeyspace> getSeaStarKeyspaces() {
		return Collections.unmodifiableMap(keyspaceById);
	}

	@Override
	@NonNull
	public Map<UUID, Node> getNodes() {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
		return getSeaStarKeyspaces().entrySet().stream()
			.collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	@Override
	@NonNull
	public Optional<TokenMap> getTokenMap() {
		throw new UnsupportedOperationException();
	}

}
