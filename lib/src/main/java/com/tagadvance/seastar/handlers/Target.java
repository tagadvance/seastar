package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.tagadvance.seastar.SeaStarKeyspace;
import com.tagadvance.seastar.SeaStarTable;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The keyspace and table a statement operates on, resolved once by {@link Targets} rather than
 * re-derived by each handler.
 *
 * @param keyspace the keyspace the statement names, or the session keyspace it fell back to
 * @param table    the table within that keyspace
 */
record Target(SeaStarKeyspace keyspace, SeaStarTable table) {

	/**
	 * The partition key columns, in key order.
	 */
	Set<CqlIdentifier> partitionKeyNames() {
		return table.getPartitionKey()
			.stream()
			.map(ColumnMetadata::getName)
			.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * Every primary key column - the partition key first, then the clustering columns - in key order.
	 * Iteration order matters where a handler reports the first missing part, so the set is ordered.
	 */
	Set<CqlIdentifier> primaryKeyNames() {
		return Stream.concat(table.getPartitionKey().stream(),
				table.getClusteringColumns().keySet().stream())
			.map(ColumnMetadata::getName)
			.collect(Collectors.toCollection(LinkedHashSet::new));
	}

}
