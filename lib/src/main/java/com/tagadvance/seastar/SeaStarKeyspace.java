package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.util.Map;
import java.util.Optional;

public interface SeaStarKeyspace extends SeaStarReadWriteLock, KeyspaceMetadata {

	SeaStarDriverContext context();

	CqlIdentifier name();

	/**
	 * Replaces the keyspace's replication options and its durable-writes flag. This is
	 * {@code ALTER KEYSPACE}, which replaces only the options the statement names, so the caller
	 * passes the values that survive alongside the ones that change.
	 */
	void alter(Map<String, String> replication, boolean durableWrites);

	default Optional<SeaStarUserDefinedType> getSeaStarUserDefinedType(final String name) {
		return getSeaStarUserDefinedType(CqlIdentifier.fromInternal(name));
	}

	Optional<SeaStarUserDefinedType> getSeaStarUserDefinedType(CqlIdentifier id);

	default void putSeaStarUserDefinedType(final String name, final SeaStarUserDefinedType Object) {
		putSeaStarUserDefinedType(CqlIdentifier.fromInternal(name), Object);
	}

	void putSeaStarUserDefinedType(CqlIdentifier id, SeaStarUserDefinedType Object);

	default void removeSeaStarUserDefinedType(final String name) {
		removeSeaStarUserDefinedType(CqlIdentifier.fromInternal(name));
	}

	void removeSeaStarUserDefinedType(CqlIdentifier id);

	Map<CqlIdentifier, SeaStarUserDefinedType> getSeaStarUserDefinedTypes();

	default Optional<SeaStarTable> getSeaStarTable(final String name) {
		return getSeaStarTable(CqlIdentifier.fromInternal(name));
	}

	Optional<SeaStarTable> getSeaStarTable(CqlIdentifier id);

	default SeaStarTable newSeaStarTable(final String name) {
		return newSeaStarTable(CqlIdentifier.fromInternal(name));
	}

	default SeaStarTable newSeaStarTable(final CqlIdentifier id) {
		final var table = new VolatileTable(context(), this, id);
		putSeaStarTable(table);

		return table;
	}

	void putSeaStarTable(SeaStarTable table);

	default void removeSeaStarTable(final String name) {
		removeSeaStarTable(CqlIdentifier.fromInternal(name));
	}

	void removeSeaStarTable(CqlIdentifier id);

	Map<CqlIdentifier, SeaStarTable> getSeaStarTables();

}
