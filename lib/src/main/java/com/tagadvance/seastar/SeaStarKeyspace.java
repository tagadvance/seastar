package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.util.Map;
import java.util.Optional;

public interface SeaStarKeyspace extends SeaStarReadWriteLock, KeyspaceMetadata {

	SeaStarDriverContext context();

	CqlIdentifier name();

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
		putSeaStarTable(id, table);

		return table;
	}

	default void putSeaStarTable(final String name, final SeaStarTable table) {
		putSeaStarTable(CqlIdentifier.fromInternal(name), table);
	}

	void putSeaStarTable(CqlIdentifier id, SeaStarTable table);

	default void removeSeaStarTable(final String name) {
		removeSeaStarTable(CqlIdentifier.fromInternal(name));
	}

	void removeSeaStarTable(CqlIdentifier id);

	Map<CqlIdentifier, SeaStarTable> getSeaStarTables();

}
