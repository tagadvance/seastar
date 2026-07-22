package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.TruncateStatement;

@ThreadSafe
public class TruncateHandler implements CqlHandler<TruncateStatement> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public TruncateHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof TruncateStatement;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final TruncateStatement raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final var name = Reflections.getDeclaredField(raw, "qualifiedName", QualifiedName.class)
			.orElseThrow();

		final var keyspace = Optional.ofNullable(name.hasKeyspace() ? name.getKeyspace() : null)
			.or(() -> getKeyspace.get().map(CqlIdentifier::asInternal))
			.orElse(null);
		if (keyspace == null) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename"));
		}

		final var table = name.getName();
		final var optionalTable = context.getSeaStarKeyspace(CqlIdentifier.fromInternal(keyspace))
			.flatMap(ksx -> ksx.getSeaStarTable(CqlIdentifier.fromInternal(table)));
		if (optionalTable.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"Table '%s.%s' doesn't exist".formatted(keyspace, table)));
		}

		optionalTable.get().truncate();

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
