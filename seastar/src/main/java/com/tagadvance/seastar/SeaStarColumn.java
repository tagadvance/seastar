package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.tagadvance.tools.SeaStarReadWriteLock;

/**
 * A column of a table: its name, type and whether it is part of the primary key or static. Carries
 * no value of its own - a row's positional {@code Cells} hold those. Guarded by its table's
 * keyspace's lock; see the lock hierarchy in {@code AGENTS.md}.
 */
public interface SeaStarColumn extends SeaStarReadWriteLock, ColumnDefinition, ColumnMetadata {

	/**
	 * The table this column belongs to, which is where {@link #lock()} comes from.
	 */
	SeaStarTable table();

}
