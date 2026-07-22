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
import org.apache.cassandra.cql3.statements.schema.DropTableStatement.Raw;

@ThreadSafe
public class DropTableHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public DropTableHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final var name = Reflections.getDeclaredField(raw, "name", QualifiedName.class).orElseThrow();
		final var ifExists = Reflections.getDeclaredField(raw, "ifExists", Boolean.class).orElse(false);

		final var keyspace = Optional.ofNullable(name.hasKeyspace() ? name.getKeyspace() : null)
			.or(() -> getKeyspace.get().map(CqlIdentifier::asInternal))
			.orElse(null);
		if (keyspace == null) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename"));
		}

		final var table = name.getName();
		final var optionalKeyspace = context.getSeaStarKeyspace(
			CqlIdentifier.fromInternal(keyspace));
		final var optionalTable = optionalKeyspace.flatMap(
			ksx -> ksx.getSeaStarTable(CqlIdentifier.fromInternal(table)));
		if (optionalTable.isEmpty()) {
			if (ifExists) {
				return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
			}
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"Table '%s.%s' doesn't exist".formatted(keyspace, table)));
		}

		optionalKeyspace.get().removeSeaStarTable(CqlIdentifier.fromInternal(table));
		optionalTable.get().drop();

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
