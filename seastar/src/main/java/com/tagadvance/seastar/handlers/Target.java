package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.tagadvance.seastar.SeaStarKeyspace;
import com.tagadvance.seastar.SeaStarTable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The keyspace and table a statement operates on, resolved once by {@link Targets} rather than
 * re-derived by each handler.
 *
 * @param keyspace the keyspace the statement names, or the session keyspace it fell back to
 * @param table    the table within that keyspace
 */
record Target(SeaStarKeyspace keyspace, SeaStarTable table) {

	// Both methods below are plain loops, not streams, on purpose: they are recomputed on every
	// single statement (called 2-3x per INSERT alone, including once per BATCH child), and
	// profiling batch100 (TODO/batch100-investigation-prompt.md) found java.util.stream's
	// pipeline setup cost - Spliterator, Sink chain, megamorphic dispatch - dominating the whole
	// per-statement budget for what is a 1-3 element collection. Do not revert to streams here.

	/**
	 * The partition key columns, in key order.
	 */
	Set<CqlIdentifier> partitionKeyNames() {
		final var names = new LinkedHashSet<CqlIdentifier>();
		for (final var column : table.getPartitionKey()) {
			names.add(column.getName());
		}

		return names;
	}

	/**
	 * Every primary key column - the partition key first, then the clustering columns - in key order.
	 * Iteration order matters where a handler reports the first missing part, so the set is ordered.
	 */
	Set<CqlIdentifier> primaryKeyNames() {
		final var names = new LinkedHashSet<CqlIdentifier>();
		for (final var column : table.getPartitionKey()) {
			names.add(column.getName());
		}
		for (final var column : table.getClusteringColumns().keySet()) {
			names.add(column.getName());
		}

		return names;
	}

}
