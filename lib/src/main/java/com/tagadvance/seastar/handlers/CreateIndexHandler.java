package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultIndexMetadata;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarTable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement.Raw;

@ThreadSafe
public class CreateIndexHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public CreateIndexHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
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

		final var tableName = FieldBindings.CREATE_INDEX_TABLE_NAME.require(raw);
		final var ifNotExists = FieldBindings.CREATE_INDEX_IF_NOT_EXISTS.require(raw);

		final var keyspace = Optional.ofNullable(
				tableName.hasKeyspace() ? tableName.getKeyspace() : null)
			.or(() -> getKeyspace.get().map(CqlIdentifier::asInternal))
			.orElse(null);
		if (keyspace == null) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename"));
		}

		final var table = tableName.getName();
		final var optionalTable = context.getSeaStarKeyspace(CqlIdentifier.fromInternal(keyspace))
			.flatMap(ksx -> ksx.getSeaStarTable(CqlIdentifier.fromInternal(table)));
		if (optionalTable.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"Table '%s.%s' doesn't exist".formatted(keyspace, table)));
		}
		final var seaStarTable = optionalTable.get();

		final var targets = FieldBindings.CREATE_INDEX_RAW_TARGETS.require(raw);
		if (targets.isEmpty()) {
			return CompletableFuture.failedStage(
				new InvalidQueryException(coordinator, "Only CREATE INDEX on a single column is supported"));
		}
		final var columnId = FieldBindings.INDEX_TARGET_COLUMN.require(targets.get(0));
		final var column = CqlIdentifier.fromInternal(columnId.toString());
		if (seaStarTable.firstIndexOf(column) < 0) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"No column definition '%s' found for table '%s'".formatted(column.asInternal(), table)));
		}

		final var indexName = resolveIndexName(raw, table, column);
		if (seaStarTable.getIndexes().containsKey(indexName)) {
			if (ifNotExists) {
				return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
			}
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"Index '%s' already exists".formatted(indexName.asInternal())));
		}

		final var target = column.asInternal();
		final var index = new DefaultIndexMetadata(CqlIdentifier.fromInternal(keyspace),
			seaStarTable.getName(), indexName, IndexKind.COMPOSITES, target,
			Map.of("target", target));
		seaStarTable.addIndex(index);

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

	private static CqlIdentifier resolveIndexName(final Raw raw, final String table,
		final CqlIdentifier column) {
		// CREATE INDEX ON ... leaves the name unset, so absence here is a valid answer.
		return FieldBindings.CREATE_INDEX_INDEX_NAME.find(raw)
			.map(QualifiedName::getName)
			.map(CqlIdentifier::fromInternal)
			// Cassandra derives an unspecified index name as <table>_<column>_idx.
			.orElseGet(() -> CqlIdentifier.fromInternal(
				"%s_%s_idx".formatted(table, column.asInternal())));
	}

}
