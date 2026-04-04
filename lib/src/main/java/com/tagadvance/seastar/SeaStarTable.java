package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

public interface SeaStarTable extends TableMetadata, ColumnDefinitions {

	SeaStarDriverContext context();

	// Rows rows(); // TODO

	void detach();

}
