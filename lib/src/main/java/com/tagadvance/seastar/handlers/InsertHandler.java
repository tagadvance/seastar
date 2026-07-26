package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;

public class InsertHandler implements CqlHandler<ParsedInsert> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public InsertHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof ParsedInsert;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final ParsedInsert raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final var keyspace = Optional.ofNullable(raw.keyspace())
			.or(() -> getKeyspace.get().map(CqlIdentifier::asInternal))
			.orElse(null);
		if (keyspace == null) {
			throw new InvalidQueryException(coordinator,
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
		}

		final var optionalKeyspace = context.getSeaStarKeyspace(
			CqlIdentifier.fromInternal(keyspace));
		if (optionalKeyspace.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"Keyspace '%s' does not exist".formatted(keyspace)));
		}

		final var optionalTable = optionalKeyspace.get()
			.getSeaStarTable(CqlIdentifier.fromInternal(raw.name()));
		if (optionalTable.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"table %s does not exist".formatted(raw.name())));
		}
		final var table = optionalTable.get();

		final var columnNames = FieldBindings.INSERT_COLUMN_NAMES.require(raw);
		final var columnValues = FieldBindings.INSERT_COLUMN_VALUES.require(raw);
		final var ifNotExists = FieldBindings.MODIFICATION_IF_NOT_EXISTS.require(raw);

		final var codecRegistry = context.getCodecRegistry();
		final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
		final var named = new HashSet<CqlIdentifier>();
		final var namedIndices = new ArrayList<Integer>(columnNames.size());
		for (int i = 0; i < columnNames.size(); i++) {
			final var name = CqlIdentifier.fromInternal(columnNames.get(i).toString());
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(name.asInternal())));
			}
			named.add(name);
			namedIndices.add(index);

			final var dataType = table.get(index).getType();
			final var term = columnValues.get(i);
			values.set(index, Terms.resolve(term, dataType, codecRegistry, coordinator, bindings));
		}

		final List<CqlIdentifier> primaryKey = new ArrayList<>();
		table.getPartitionKey().stream().map(ColumnMetadata::getName).forEach(primaryKey::add);
		table.getClusteringColumns().keySet().stream().map(ColumnMetadata::getName)
			.forEach(primaryKey::add);
		for (final var pk : primaryKey) {
			if (!named.contains(pk)) {
				return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
					"Missing mandatory PRIMARY KEY part %s".formatted(pk.asInternal())));
			}
		}

		final var pkIndices = primaryKey.stream().mapToInt(table::firstIndexOf).toArray();
		final Predicate<SeaStarRow> samePrimaryKey = existing -> {
			for (final var index : pkIndices) {
				if (!Objects.equals(existing.getObject(index), values.get(index))) {
					return false;
				}
			}
			return true;
		};

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			if (ifNotExists) {
				final var existing = table.rows().filter(samePrimaryKey).findFirst().orElse(null);
				if (existing == null) {
					table.addRow(values);
					return AppliedResultSets.of(context, table, executionInfo, true);
				}
				return AppliedResultSets.ofExisting(context, table, executionInfo, existing.snapshot());
			}
			// INSERT is an upsert; write only the named columns, preserving any columns this
			// statement did not specify on a row that already shares this primary key. An
			// explicitly-inserted NULL still clears its column; an unnamed column is left as-is.
			final var existing = table.rows().filter(samePrimaryKey).findFirst().orElse(null);
			if (existing == null) {
				table.addRow(values);
			} else {
				for (final var index : namedIndices) {
					existing.set(index, values.get(index));
				}
			}
			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

}
