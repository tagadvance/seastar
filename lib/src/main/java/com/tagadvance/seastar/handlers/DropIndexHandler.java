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
import org.apache.cassandra.cql3.statements.schema.DropIndexStatement.Raw;
import org.jspecify.annotations.Nullable;

/**
 * Handles {@code DROP INDEX}. Mirrors {@code DropIndexStatement}: an index is named by keyspace
 * alone, so the table that owns it is found by looking through the keyspace, and a keyspace that
 * does not exist is reported as an index that does not exist - which is what a live node does,
 * because it looks the index up rather than the keyspace.
 */
@ThreadSafe
public class DropIndexHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public DropIndexHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var node = executionInfo.getCoordinator();
		final var name = FieldBindings.DROP_INDEX_NAME.require(raw);
		final var ifExists = FieldBindings.DROP_INDEX_IF_EXISTS.require(raw);

		final CqlIdentifier keyspaceName;
		try {
			keyspaceName = Targets.requireKeyspaceName(getKeyspace, keyspaceOf(name), node);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var indexName = CqlIdentifier.fromInternal(name.getName());
		final var table = context.getSeaStarKeyspace(keyspaceName)
			.stream()
			.flatMap(keyspace -> keyspace.getSeaStarTables().values().stream())
			.filter(candidate -> candidate.getIndexes().containsKey(indexName))
			.findFirst();
		if (table.isEmpty()) {
			if (ifExists) {
				return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
			}

			return CompletableFuture.failedStage(new InvalidQueryException(node,
				"Index '%s.%s' doesn't exist".formatted(keyspaceName.asInternal(),
					indexName.asInternal())));
		}

		table.ifPresent(owner -> owner.removeIndex(indexName));

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

	private static @Nullable String keyspaceOf(final QualifiedName name) {
		return name.hasKeyspace() ? name.getKeyspace() : null;
	}

}
