package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement.Parsed;

@ThreadSafe
public class DeleteHandler implements CqlHandler<Parsed> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public DeleteHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof Parsed;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final Parsed raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final Modification delete;
		final Predicate<SeaStarRow> predicate;
		try {
			delete = Modifications.delete(context, getKeyspace, raw, coordinator, bindings);
			validateDeletedColumns(delete, coordinator);
			predicate = RestrictionRules.forDelete(delete, coordinator);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var table = delete.target().table();
		final var deletedColumns = delete.assignments();
		final var conditions = delete.conditions();
		final var writes = Writes.of(context, delete);
		// A DELETE that names no clustering column removes the partition, and a partition's static
		// columns go with it; one that reaches a single row leaves them alone.
		final var partitionWide = deletedColumns.isEmpty() && !restrictsClustering(delete);
		final var stamped = delete.timestamp() != null;

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			final var matched = table.rows().filter(SeaStarRow::isLive).filter(predicate).toList();

			if (delete.ifExists()) {
				if (matched.isEmpty()) {
					return AppliedResultSets.of(context, table, executionInfo, false);
				}
				applyDelete(table, matched, deletedColumns, partitionWide, writes, stamped, coordinator);

				return AppliedResultSets.of(context, table, executionInfo, true);
			}

			if (!conditions.isEmpty()) {
				if (matched.isEmpty()) {
					return AppliedResultSets.of(context, table, executionInfo, false);
				}
				final var existing = matched.get(0).snapshot();
				if (!Conditions.hold(conditions, existing)) {
					return AppliedResultSets.ofExisting(context, table, executionInfo, existing);
				}
				applyDelete(table, matched, deletedColumns, partitionWide, writes, stamped, coordinator);

				return AppliedResultSets.of(context, table, executionInfo, true);
			}

			applyDelete(table, matched, deletedColumns, partitionWide, writes, stamped, coordinator);

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	/**
	 * A DELETE that names no column removes the whole row; one that names columns clears them and
	 * leaves the row in place.
	 */
	private static void applyDelete(final SeaStarTable table, final List<SeaStarRow> matched,
		final List<Assignment> deletedColumns, final boolean partitionWide, final Writes writes,
		final boolean stamped, final Node coordinator) {
		if (!deletedColumns.isEmpty()) {
			for (final var row : matched) {
				for (final var deleted : deletedColumns) {
					final var index = deleted.columnIndex();
					row.set(index, deleted.apply(row.getObject(index), coordinator),
						writes.timestamp(), Writes.NEVER);
				}
			}

			return;
		}

		// Static cells live with the partition rather than with any row, so removing the rows would
		// leave them behind for the next write to the same partition to read back.
		final var cleared = partitionWide ? nonKeyIndices(table) : nonStaticNonKeyIndices(table);
		matched.forEach(row -> cleared.forEach(
			index -> row.set(index, null, writes.timestamp(), Writes.NEVER)));
		matched.forEach(row -> row.clearMarker(writes.timestamp()));
		// A delete stamped with a timestamp of its own removes only what it is newer than, so a row
		// holding a value written after it survives; an unstamped one is the newest write there is.
		table.removeRowIf(stamped ? row -> matched.contains(row) && !row.isLive()
			: matched::contains);
	}

	private static List<Integer> nonKeyIndices(final SeaStarTable table) {
		final var primaryKey = Stream.concat(table.getPartitionKey().stream(),
				table.getClusteringColumns().keySet().stream())
			.map(ColumnMetadata::getName)
			.collect(Collectors.toSet());

		return IntStream.range(0, table.size())
			.filter(index -> !primaryKey.contains(table.get(index).getName()))
			.boxed()
			.toList();
	}

	private static List<Integer> nonStaticNonKeyIndices(final SeaStarTable table) {
		return nonKeyIndices(table).stream()
			.filter(index -> !(table.get(index) instanceof ColumnMetadata column
				&& column.isStatic()))
			.toList();
	}

	private static boolean restrictsClustering(final Modification delete) {
		final var table = delete.target().table();
		final var clustering = table.getClusteringColumns()
			.keySet()
			.stream()
			.map(ColumnMetadata::getName)
			.collect(Collectors.toSet());

		return delete.restrictions()
			.stream()
			.flatMap(restriction -> restriction.columns().stream())
			.map(Restriction.Column::name)
			.anyMatch(clustering::contains);
	}

	private static void validateDeletedColumns(final Modification delete, final Node coordinator) {
		final var primaryKey = delete.target().primaryKeyNames();
		for (final var deleted : delete.assignments()) {
			if (primaryKey.contains(deleted.column())) {
				throw new InvalidQueryException(coordinator,
					"Invalid identifier %s for deletion (should not be a PRIMARY KEY part)".formatted(
						deleted.column().asInternal()));
			}
		}
	}

}
