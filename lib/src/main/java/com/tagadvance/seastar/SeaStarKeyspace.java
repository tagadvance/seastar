package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.tagadvance.seastar.VolatileTable.VolatileColumn;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface SeaStarKeyspace extends KeyspaceMetadata {

	SeaStarDriverContext context();

	CqlIdentifier name();

	Optional<SeaStarUserDefinedType> getSeaStarUserDefinedType(CqlIdentifier id);

	void putSeaStarUserDefinedType(CqlIdentifier id, SeaStarUserDefinedType Object);

	void removeSeaStarUserDefinedType(CqlIdentifier id);

	Map<CqlIdentifier, SeaStarUserDefinedType> getSeaStarUserDefinedTypes();

	Optional<SeaStarTable> getSeaStarTable(CqlIdentifier id);

	default SeaStarTable newSeaStarTable(final CqlIdentifier id,
		final List<VolatileColumn> columns) {
		final var table = new VolatileTable(context(), name(), id, columns, false);
		putSeaStarTable(id, table);

		return table;
	}

	void putSeaStarTable(CqlIdentifier id, SeaStarTable Table);

	void removeSeaStarTable(CqlIdentifier id);

	Map<CqlIdentifier, SeaStarTable> getSeaStarTables();

}
