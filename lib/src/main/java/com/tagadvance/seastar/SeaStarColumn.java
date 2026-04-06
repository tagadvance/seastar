package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.tagadvance.tools.SeaStarReadWriteLock;

public interface SeaStarColumn extends SeaStarReadWriteLock, ColumnDefinition, ColumnMetadata {

	SeaStarTable table();

}
