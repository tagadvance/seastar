package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import com.tagadvance.seastar.SeaStarAsyncResultSet;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import java.util.LinkedList;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.CQLStatement;

public interface CqlHandler<T extends CQLStatement.Raw> {

	boolean canProcess(final CQLStatement.Raw raw);

	CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, T raw, final Object... bindings);

	default AsyncResultSet newAsyncResultSet(final ExecutionInfo executionInfo) {
		return newAsyncResultSet(EmptyColumnDefinitions.INSTANCE, executionInfo, Stream.empty());
	}

	default AsyncResultSet newAsyncResultSet(final ColumnDefinitions columnDefinitions,
		final ExecutionInfo executionInfo, final Stream<SeaStarRow> rows) {
		final var data = rows.map(SeaStarRow::snapshot)
			.collect(Collectors.toCollection(LinkedList::new));

		return new SeaStarAsyncResultSet(columnDefinitions, executionInfo, data);
	}

}
