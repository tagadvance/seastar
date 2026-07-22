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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.masking.ColumnMask;
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
	@SuppressWarnings("unchecked")
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
		final var ifNotExists = Reflections.getDeclaredField(raw, "ifNotExists", Boolean.class).orElse(false);
		final var useCompactStorage = Reflections.getDeclaredField(raw, "useCompactStorage",
			Boolean.class).orElse(false);

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

			final Map<ColumnIdentifier, Object> rawColumns = Reflections.getDeclaredField(raw,
				"rawColumns", Map.class).orElseGet(Collections::emptyMap);
			final List<ColumnIdentifier> partitionKeyColumns = Reflections.getDeclaredField(raw,
				"partitionKeyColumns", List.class).orElseGet(Collections::emptyList);
			final List<ColumnIdentifier> clusteringColumns = Reflections.getDeclaredField(raw,
				"clusteringColumns", List.class).orElseGet(Collections::emptyList);
			final Map<ColumnIdentifier, Boolean> clusteringOrder = Reflections.getDeclaredField(raw,
				"clusteringOrder", Map.class).orElseGet(Collections::emptyMap);
			final Set<ColumnIdentifier> staticColumns = Reflections.getDeclaredField(raw,
				"staticColumns", Set.class).orElseGet(Collections::emptySet);

			rawColumns.values().forEach(value ->
				Reflections.getDeclaredField(value, "rawMask", ColumnMask.Raw.class).ifPresent(mask -> {
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
				final var dataType = Reflections.getDeclaredField(value, "rawType", Object.class)
					.map(SeaStarRawType::from)
					.flatMap(SeaStarRawType::toDataType);
				if (dataType.isEmpty()) {
					// FIXME: collections, UDTs, tuples, and vectors are not yet persisted.
					LOG.warn("Skipping column '{}' with unsupported type", key);
					continue;
				}
				final var name = CqlIdentifier.fromInternal(key.toString()); // TODO: ensure valid name
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
