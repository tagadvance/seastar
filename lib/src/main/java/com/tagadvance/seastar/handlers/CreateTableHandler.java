package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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

			final Map<ColumnIdentifier, Object> rawColumns = Reflections.getDeclaredField(raw, "rawColumns",
				Map.class).orElseGet(Collections::emptyMap);
			rawColumns.forEach((key, value) -> {
				Reflections.getDeclaredField(value, "rawMask", ColumnMask.Raw.class).ifPresent(mask -> {
					throw new UnsupportedOperationException("Column masks are not supported");
				});

				Reflections.getDeclaredField(value, "rawType", Object.class)
					.map(SeaStarRawType::from)
					.ifPresent(rawType -> {
					final var type = rawType.type();
					final var frozen = rawType.isFrozen();

					final var string = key.toString(); // TODO: ensure valid name
					final var cqlIdentifier = CqlIdentifier.fromInternal(string);

					//table1.addColumn(cqlIdentifier, type);
					System.gc();
				});
			});
			// FIXME: columns, keys, and clustering columns
			// assending default
		}

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

}
