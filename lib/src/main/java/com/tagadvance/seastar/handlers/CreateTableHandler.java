package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement.Raw;

public class CreateTableHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public CreateTableHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Raw;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Raw raw, final Object... bindings) {
		final var keyspace = Optional.of(raw)
			.map(Raw::keyspace)
			.orElseGet(() -> getKeyspace.get().map(CqlIdentifier::asInternal).orElse(null));
		if (keyspace == null) {
			throw new InvalidQueryException(executionInfo.getCoordinator(),
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
		}

		final var table = raw.table();
		final var ifNotExists = FieldBindings.CREATE_TABLE_IF_NOT_EXISTS.require(raw);
		final var useCompactStorage = FieldBindings.CREATE_TABLE_USE_COMPACT_STORAGE.require(raw);

		if (useCompactStorage) {
			throw new UnsupportedOperationException("COMPACT STORAGE is not supported");
		}

		final var optionalKeyspace = context.getSeaStarKeyspace(keyspace);
		if (optionalKeyspace.isEmpty()) {
			throw new InvalidQueryException(executionInfo.getCoordinator(),
				"Keyspace '%s' does not exist".formatted(keyspace));
		}

		final var ksx = optionalKeyspace.get();
		final var optionalTable = ksx.getSeaStarTable(table);
		if (optionalTable.isPresent()) {
			if (ifNotExists) {
				LOG.debug("Table {} in keyspace {} already exists, skipping creation", table,
					keyspace);
			} else {
				return CompletableFuture.failedStage(
					new AlreadyExistsException(executionInfo.getCoordinator(), keyspace, table));
			}
		} else {
			final var table1 = ksx.newSeaStarTable(table);

			final var rawColumns = FieldBindings.CREATE_TABLE_RAW_COLUMNS.require(raw);
			final var partitionKeyColumns = FieldBindings.CREATE_TABLE_PARTITION_KEY_COLUMNS.require(
				raw);
			final var clusteringColumns = FieldBindings.CREATE_TABLE_CLUSTERING_COLUMNS.require(raw);
			final var clusteringOrder = FieldBindings.CREATE_TABLE_CLUSTERING_ORDER.require(raw);
			final var staticColumns = FieldBindings.CREATE_TABLE_STATIC_COLUMNS.require(raw);

			// A mask is only present on a column declared with MASKED WITH.
			rawColumns.values().forEach(value ->
				FieldBindings.COLUMN_RAW_MASK.find(value).ifPresent(mask -> {
					throw new UnsupportedOperationException("Column masks are not supported");
				}));

			// Cassandra orders columns partition key, then clustering, then the rest alphabetically.
			final List<ColumnIdentifier> ordered = new ArrayList<>(partitionKeyColumns);
			ordered.addAll(clusteringColumns);
			rawColumns.keySet().stream()
				.filter(key -> !partitionKeyColumns.contains(key) && !clusteringColumns.contains(key))
				.sorted(Comparator.comparing(ColumnIdentifier::toString))
				.forEach(ordered::add);

			for (final var key : ordered) {
				final var value = rawColumns.get(key);
				final var rawType = FieldBindings.COLUMN_RAW_TYPE.require(value);
				final var dataType = new SeaStarRawType(rawType).toDataType(ksx,
					executionInfo.getCoordinator());
				if (dataType.isEmpty()) {
					throw new InvalidQueryException(executionInfo.getCoordinator(),
						"Unknown type for column '%s'".formatted(key));
				}
				final var name = CqlIdentifier.fromInternal(key.toString());
				table1.addColumn(name, dataType.get(), staticColumns.contains(key));
			}

			partitionKeyColumns.forEach(key ->
				table1.markPartitionKey(CqlIdentifier.fromInternal(key.toString())));
			clusteringColumns.forEach(key -> {
				final boolean ascending = clusteringOrder.getOrDefault(key, Boolean.TRUE);
				table1.markClustering(CqlIdentifier.fromInternal(key.toString()),
					ascending ? ClusteringOrder.ASC : ClusteringOrder.DESC);
			});
		}

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
