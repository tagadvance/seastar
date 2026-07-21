package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement.Raw;

public class CreateKeyspaceHandler implements CqlHandler<CreateKeyspaceStatement.Raw> {

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof CreateKeyspaceStatement.Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var name = raw.keyspaceName;
		final var ifNotExists = Reflections.getDeclaredField(raw, "ifNotExists", Boolean.class).orElse(false);
		final var optionalKeyspace = context.getSeaStarKeyspace(name);
		if (optionalKeyspace.isPresent()) {
			if (ifNotExists) {
				LOG.debug("Keyspace {} already exists, skipping creation", name);
			} else {
				return CompletableFuture.failedStage(
					new AlreadyExistsException(executionInfo.getCoordinator(), name, null));
			}
		} else {
			context.newSeaStarKeyspace(name);
		}

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
