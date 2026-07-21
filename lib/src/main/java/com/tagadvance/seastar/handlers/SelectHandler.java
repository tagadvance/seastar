package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.SelectStatement.RawStatement;

public class SelectHandler implements CqlHandler<RawStatement> {

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof RawStatement;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final RawStatement raw, final Object... bindings) {
		final var isDistinct = raw.parameters.isDistinct;

		return context.getSeaStarKeyspace(
				CqlIdentifier.fromInternal(raw.keyspace()))
			.flatMap(keyspace -> keyspace.getSeaStarTable(
				CqlIdentifier.fromInternal(raw.name())))
			.map(table -> {
				if (isDistinct) {
					LOG.warn("DISTINCT is not supported, ignoring");
				}

				// ignore select clause because we always return everything
				// TODO: where clause filtering
				// TODO: read lock on table
				var rows = table.rows();

				return CompletableFuture.completedStage(
					newAsyncResultSet(table, executionInfo, rows));
			})
			.orElseGet(
				() -> CompletableFuture.failedStage(new UnsupportedOperationException()));
	}

}
