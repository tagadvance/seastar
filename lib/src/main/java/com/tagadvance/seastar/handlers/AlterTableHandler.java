package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.schema.AlterTableStatement.Raw;
import org.jspecify.annotations.Nullable;

/**
 * Handles {@code ALTER TABLE}. Mirrors {@code AlterTableStatement}: columns may be added, dropped or
 * - if they are part of the primary key - renamed, and altering a column's type is rejected the way
 * Cassandra 5 rejects it.
 *
 * <p>A row holds its values positionally, indexed by the table's column list, so adding a column
 * opens a slot in every existing row and dropping one closes it. Both happen under the table's write
 * lock, in {@code VolatileTable}, so no reader can see a row whose length disagrees with the column
 * list. Dropping discards the values that column held, which is what a live node does too: re-adding
 * the column brings it back empty.
 *
 * <p>{@code WITH} options are accepted and ignored. SeaStar models no table options at all -
 * {@code TableMetadata#getOptions()} is always empty, whether the table was created with options or
 * altered to have them - so accepting the statement keeps SeaStar's answer the same as a cluster's
 * for everything it does model, and rejecting it would fail schema scripts over settings that cannot
 * change any result.
 */
@ThreadSafe
public class AlterTableHandler implements CqlHandler<Raw> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public AlterTableHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
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
		final var name = FieldBindings.ALTER_TABLE_NAME.require(raw);
		final var ifTableExists = FieldBindings.ALTER_TABLE_IF_TABLE_EXISTS.require(raw);

		final Target target;
		try {
			// Cassandra resolves the keyspace before it consults IF EXISTS, so a statement that names
			// no keyspace at all fails even when the table is allowed to be missing.
			Targets.requireKeyspaceName(getKeyspace, keyspaceOf(name), node);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}
		try {
			target = Targets.require(context, getKeyspace, name, node);
		} catch (final InvalidQueryException e) {
			return ifTableExists ? CompletableFuture.completedStage(newAsyncResultSet(executionInfo))
				: CompletableFuture.failedStage(e);
		}

		final var kind = FieldBindings.ALTER_TABLE_KIND.require(raw).name();
		try {
			final var result = switch (kind) {
				case "ADD_COLUMNS" -> addColumns(executionInfo, raw, target);
				case "DROP_COLUMNS" -> dropColumns(executionInfo, raw, target);
				case "RENAME_COLUMNS" -> renameColumns(executionInfo, raw, target);
				// Table options are not modelled; see the class javadoc.
				case "ALTER_OPTIONS" -> newAsyncResultSet(executionInfo);
				case "ALTER_COLUMN" -> throw new InvalidQueryException(node,
					"Altering column types is no longer supported");
				case "MASK_COLUMN" -> throw new InvalidQueryException(node,
					"SeaStar does not support column masks");
				default -> throw new InvalidQueryException(node,
					"SeaStar does not support ALTER TABLE ... %s".formatted(
						kind.replace('_', ' ')));
			};

			return CompletableFuture.completedStage(result);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}
	}

	private AsyncResultSet addColumns(final ExecutionInfo executionInfo, final Raw raw,
		final Target target) {
		final var node = executionInfo.getCoordinator();
		final var ifColumnNotExists = FieldBindings.ALTER_TABLE_IF_COLUMN_NOT_EXISTS.require(raw);
		final var added = FieldBindings.ALTER_TABLE_ADDED_COLUMNS.require(raw);
		final var table = target.table();

		return table.writeLockUnchecked(() -> {
			// Validated in full before anything is applied, so a rejected statement leaves the table
			// as it was rather than half altered.
			final List<Column> columns = new ArrayList<>();
			for (final var column : added) {
				final var name = identifier(FieldBindings.ADDED_COLUMN_NAME.require(column));
				final var isStatic = FieldBindings.ADDED_COLUMN_IS_STATIC.require(column);
				// A mask is only present on a column declared with MASKED WITH.
				FieldBindings.ADDED_COLUMN_MASK.find(column).ifPresent(mask -> {
					throw new InvalidQueryException(node, "SeaStar does not support column masks");
				});

				if (table.firstIndexOf(name) >= 0) {
					if (ifColumnNotExists) {
						continue;
					}

					throw new InvalidQueryException(node,
						"Column with name '%s' already exists".formatted(name.asInternal()));
				}
				if (isStatic && table.getClusteringColumns().isEmpty()) {
					throw new InvalidQueryException(node, "Static columns are only useful (and thus "
						+ "allowed) if the table has at least one clustering column");
				}

				final var rawType = new SeaStarRawType(
					FieldBindings.ADDED_COLUMN_TYPE.require(column));
				final var dataType = rawType.toDataType(target.keyspace(), node)
					.orElseThrow(() -> new InvalidQueryException(node,
						"Unknown type for column '%s'".formatted(name.asInternal())));
				columns.add(new Column(name, dataType, isStatic));
			}

			columns.forEach(
				column -> table.insertColumn(column.name(), column.type(), column.isStatic()));

			return applied(executionInfo, target, !columns.isEmpty());
		});
	}

	private AsyncResultSet dropColumns(final ExecutionInfo executionInfo, final Raw raw,
		final Target target) {
		final var node = executionInfo.getCoordinator();
		final var ifColumnExists = FieldBindings.ALTER_TABLE_IF_COLUMN_EXISTS.require(raw);
		final var dropped = FieldBindings.ALTER_TABLE_DROPPED_COLUMNS.require(raw);
		final var table = target.table();

		return table.writeLockUnchecked(() -> {
			final var primaryKey = target.primaryKeyNames();
			final List<CqlIdentifier> columns = new ArrayList<>();
			for (final var column : dropped) {
				final var name = identifier(column);
				// Checked before the column is looked up: IF EXISTS does not make dropping a key
				// column legal, it only forgives a column that is not there.
				if (primaryKey.contains(name)) {
					throw new InvalidQueryException(node,
						"Cannot drop PRIMARY KEY column %s".formatted(name.asInternal()));
				}
				if (table.firstIndexOf(name) < 0) {
					if (ifColumnExists) {
						continue;
					}

					throw new InvalidQueryException(node,
						"Column %s was not found in table '%s'".formatted(name.asInternal(),
							qualified(target)));
				}
				columns.add(name);
			}

			columns.forEach(table::removeColumn);

			return applied(executionInfo, target, !columns.isEmpty());
		});
	}

	private AsyncResultSet renameColumns(final ExecutionInfo executionInfo, final Raw raw,
		final Target target) {
		final var node = executionInfo.getCoordinator();
		final var renamed = FieldBindings.ALTER_TABLE_RENAMED_COLUMNS.require(raw);
		final var table = target.table();

		return table.writeLockUnchecked(() -> {
			final var primaryKey = target.primaryKeyNames();
			final var indexes = table.getIndexes().values();
			final Map<CqlIdentifier, CqlIdentifier> renames = new LinkedHashMap<>();
			renamed.forEach((from, to) -> {
				final var current = identifier(from);
				final var next = identifier(to);
				if (table.firstIndexOf(current) < 0) {
					// RENAME IF EXISTS does not forgive this: Cassandra reads the column through
					// getExistingColumn, which throws before the flag is consulted. Verified
					// against 5.0.8.
					throw new InvalidQueryException(node,
						"Undefined column name %s in table %s".formatted(current.asInternal(),
							qualified(target)));
				}
				if (!primaryKey.contains(current)) {
					throw new InvalidQueryException(node,
						"Cannot rename non PRIMARY KEY column %s".formatted(current.asInternal()));
				}
				if (table.firstIndexOf(next) >= 0) {
					throw new InvalidQueryException(node,
						("Cannot rename column %s to %s in table '%s'; another column with that name "
							+ "already exists").formatted(current.asInternal(), next.asInternal(),
							qualified(target)));
				}

				final var dependent = indexes.stream()
					.filter(index -> current.asInternal().equals(index.getTarget()))
					.map(IndexMetadata::getName)
					.map(CqlIdentifier::asInternal)
					.collect(Collectors.joining(", "));
				if (!dependent.isEmpty()) {
					throw new InvalidQueryException(node,
						"Can't rename column %s because it has dependent secondary indexes (%s)".formatted(
							current.asInternal(), dependent));
				}

				renames.put(current, next);
			});

			renames.forEach(table::renameColumn);

			return applied(executionInfo, target, !renames.isEmpty());
		});
	}

	/**
	 * Announces the change, so that prepared statements naming this table are re-resolved rather than
	 * answering with the columns the table had when they were prepared.
	 */
	private AsyncResultSet applied(final ExecutionInfo executionInfo, final Target target,
		final boolean changed) {
		if (changed) {
			SchemaChanges.tableChanged(target.table().context(), target.table());
		}

		return newAsyncResultSet(executionInfo);
	}

	private static String qualified(final Target target) {
		return "%s.%s".formatted(target.keyspace().name().asInternal(),
			target.table().getName().asInternal());
	}

	private static CqlIdentifier identifier(final ColumnIdentifier column) {
		return CqlIdentifier.fromInternal(column.toString());
	}

	private static @Nullable String keyspaceOf(final QualifiedName name) {
		return name.hasKeyspace() ? name.getKeyspace() : null;
	}

	private record Column(CqlIdentifier name, DataType type, boolean isStatic) {

	}

}
