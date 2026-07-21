package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.UseStatement;

public class UseKeyspaceHandler implements CqlHandler<UseStatement> {

	private final Consumer<CqlIdentifier> setKeyspace;

	public UseKeyspaceHandler(final Consumer<CqlIdentifier> setKeyspace) {
		this.setKeyspace = requireNonNull(setKeyspace, "setKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof UseStatement;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final UseStatement raw, final Object... bindings) {
		final var keyspace = raw.keyspace();
		final var cqlIdentifier = CqlIdentifier.fromInternal(keyspace);

		final var optionalKeyspace = context.getSeaStarKeyspace(cqlIdentifier);
		if (optionalKeyspace.isEmpty()) {
			throw new InvalidQueryException(executionInfo.getCoordinator(),
				"Keyspace '%s' does not exist".formatted(keyspace));
		}

		setKeyspace.accept(cqlIdentifier);

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
