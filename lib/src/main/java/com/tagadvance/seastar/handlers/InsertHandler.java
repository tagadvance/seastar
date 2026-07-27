package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsertJson;

/**
 * {@code INSERT}, written either as a column list or as a JSON document. The two differ only in how
 * the columns are named, so both are translated into the same {@link Modification} and applied here
 * once.
 */
@ThreadSafe
public class InsertHandler implements CqlHandler<ModificationStatement.Parsed> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public InsertHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof ParsedInsert || raw instanceof ParsedInsertJson;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final ModificationStatement.Parsed raw,
		final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final Modification insert;
		try {
			insert = raw instanceof ParsedInsertJson json
				? Modifications.insertJson(context, getKeyspace, json, coordinator, bindings)
				: Modifications.insert(context, getKeyspace, (ParsedInsert) raw, coordinator,
					bindings);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var target = insert.target();
		final var table = target.table();
		final var assignments = insert.assignments();
		final var writes = Writes.of(context, insert);

		final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
		assignments.forEach(assignment -> values.set(assignment.columnIndex(), assignment.value()));

		final var named = assignments.stream().map(Assignment::column).collect(Collectors.toSet());
		// A statement that writes nothing outside the partition's static columns addresses the
		// partition rather than a row in it, so it need not name the clustering key.
		final var required = writesOnlyStatics(target, assignments) ? target.partitionKeyNames()
			: target.primaryKeyNames();
		for (final var part : required) {
			if (!named.contains(part)) {
				return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
					"Missing mandatory PRIMARY KEY part %s".formatted(part.asInternal())));
			}
		}

		final var primaryKey = target.primaryKeyNames();
		final var pkIndices = primaryKey.stream().mapToInt(table::firstIndexOf).toArray();
		final Predicate<SeaStarRow> samePrimaryKey = existing -> {
			for (final var index : pkIndices) {
				if (!Objects.equals(existing.getObject(index), values.get(index))) {
					return false;
				}
			}

			return true;
		};
		// The statement names the whole partition key, so the row it would replace can only be in one
		// partition. Without this an insert walks every row in the table and a bulk load is O(n^2).
		final var partition = partitionKey(target, values);

		final AsyncResultSet result = table.mutate(() -> {
			final var existing = table.partition(partition)
				.filter(samePrimaryKey)
				.findFirst()
				.orElse(null);
			if (insert.ifNotExists()) {
				if (existing == null) {
					create(table, target, values, assignments, writes);

					return AppliedResultSets.of(context, table, executionInfo, true);
				}

				return AppliedResultSets.ofExisting(context, table, executionInfo,
					existing.snapshot());
			}
			// INSERT is an upsert; write only the named columns, preserving any columns this
			// statement did not specify on a row that already shares this primary key. An
			// explicitly-inserted NULL still clears its column; an unnamed column is left as-is.
			if (existing == null) {
				create(table, target, values, assignments, writes);
			} else {
				apply(target, existing, assignments, writes);
				existing.markLive(writes.timestamp(), writes.expiresAt());
			}

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	/**
	 * Adds the row an INSERT creates and stamps it with the statement's write time and TTL. The row
	 * marker takes the TTL too, which is what makes {@code INSERT ... USING TTL} take the whole row
	 * away rather than leaving a key with nothing under it.
	 */
	private static void create(final SeaStarTable table, final Target target,
		final List<Object> values, final List<Assignment> assignments, final Writes writes) {
		final var row = table.addRow(values, writes.timestamp());
		apply(target, row, assignments, writes);
		row.markLive(writes.timestamp(), writes.expiresAt());
		removeStaticRow(table, target, row);
	}

	private static void apply(final Target target, final SeaStarRow row,
		final List<Assignment> assignments, final Writes writes) {
		final var primaryKey = target.primaryKeyNames();
		for (final var assignment : assignments) {
			// A primary key part is not a cell and does not expire; giving it the statement's TTL
			// would take the row's identity away before the row itself.
			final var cell = primaryKey.contains(assignment.column()) ? writes.withoutExpiry()
				: writes;
			row.set(assignment.columnIndex(), assignment.value(), cell.timestamp(),
				cell.expiresAt());
		}
	}

	/**
	 * Whether the statement writes only static columns, which is what lets it leave the clustering
	 * key out.
	 */
	private static boolean writesOnlyStatics(final Target target,
		final List<Assignment> assignments) {
		final var primaryKey = target.primaryKeyNames();
		final var table = target.table();
		final var written = assignments.stream()
			.filter(assignment -> !primaryKey.contains(assignment.column()))
			.toList();

		return !written.isEmpty() && written.stream()
			.allMatch(assignment -> table.get(assignment.columnIndex()) instanceof ColumnMetadata
				column && column.isStatic());
	}

	/**
	 * Drops the partition's static row once the partition has a real one.
	 *
	 * <p>A partition whose only write named static columns reads back on a cluster as a single row
	 * with a null clustering key. The moment a clustered row is written, that row is what the
	 * partition answers with and the static row is no longer separate - the static values themselves
	 * are stored per partition, so nothing is lost by removing it.
	 */
	private static void removeStaticRow(final SeaStarTable table, final Target target,
		final SeaStarRow added) {
		if (!table.hasStaticColumns() || table.getClusteringColumns().isEmpty()
			|| isStaticRow(table, added)) {
			return;
		}

		table.removeRowIf(partitionValues(target, added),
			row -> row != added && isStaticRow(table, row));
	}

	/**
	 * The partition the values a statement writes belong to: its partition key column values, in key
	 * order.
	 */
	private static List<Object> partitionKey(final Target target, final List<Object> values) {
		final var table = target.table();

		return target.partitionKeyNames()
			.stream()
			.mapToInt(table::firstIndexOf)
			.mapToObj(values::get)
			.collect(Collectors.toCollection(ArrayList::new));
	}

	private static boolean isStaticRow(final SeaStarTable table, final SeaStarRow row) {
		return table.getClusteringColumns()
			.keySet()
			.stream()
			.map(ColumnMetadata::getName)
			.mapToInt(table::firstIndexOf)
			.allMatch(index -> row.getObject(index) == null);
	}

	private static List<Object> partitionValues(final Target target, final SeaStarRow row) {
		final var table = target.table();

		return target.partitionKeyNames()
			.stream()
			.mapToInt(table::firstIndexOf)
			.mapToObj(row::getObject)
			.toList();
	}

}
