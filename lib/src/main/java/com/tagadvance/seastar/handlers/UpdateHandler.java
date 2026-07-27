package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedUpdate;

@ThreadSafe
public class UpdateHandler implements CqlHandler<ParsedUpdate> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public UpdateHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof ParsedUpdate;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final ParsedUpdate raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final Modification update;
		final Predicate<SeaStarRow> predicate;
		final Map<Integer, Object> upsertKey;
		try {
			update = Modifications.update(context, getKeyspace, raw, coordinator, bindings);
			validateAssignments(update, coordinator);
			predicate = RestrictionRules.forUpdate(update, coordinator);
			upsertKey = RestrictionRules.upsertKey(update);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var table = update.target().table();
		final var assignments = update.assignments();
		final var conditions = update.conditions();
		final var writes = Writes.of(context, update);

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			final var matched = table.rows().filter(SeaStarRow::isLive).filter(predicate).toList();

			if (update.ifExists()) {
				if (matched.isEmpty()) {
					return AppliedResultSets.of(context, table, executionInfo, false);
				}
				apply(matched, assignments, writes, coordinator);

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
				apply(matched, assignments, writes, coordinator);

				return AppliedResultSets.of(context, table, executionInfo, true);
			}

			if (!matched.isEmpty()) {
				apply(matched, assignments, writes, coordinator);
			} else if (upsertKey != null) {
				final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
				upsertKey.forEach(values::set);
				final var row = table.addRow(values, writes.timestamp());
				apply(List.of(row), assignments, writes, coordinator);
				row.markLive(writes.timestamp(), writes.expiresAt());
			}

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	private static void apply(final List<SeaStarRow> matched, final List<Assignment> assignments,
		final Writes writes, final Node coordinator) {
		for (final var row : matched) {
			for (final var assignment : assignments) {
				final var index = assignment.columnIndex();
				row.set(index, assignment.apply(row.getObject(index), coordinator),
					writes.timestamp(), writes.expiresAt());
			}
		}
	}

	private static void validateAssignments(final Modification update, final Node coordinator) {
		final var target = update.target();
		final var primaryKey = target.primaryKeyNames();
		for (final var assignment : update.assignments()) {
			if (primaryKey.contains(assignment.column())) {
				throw new InvalidQueryException(coordinator,
					"PRIMARY KEY part %s found in SET part".formatted(
						assignment.column().asInternal()));
			}
		}
		requireNoClusteringForStatics(update, coordinator);
	}

	/**
	 * A static column belongs to the partition, so an UPDATE that writes nothing else addresses the
	 * partition and naming a clustering column would say otherwise. Cassandra refuses it rather than
	 * quietly writing the whole partition, and so does SeaStar.
	 */
	private static void requireNoClusteringForStatics(final Modification update,
		final Node coordinator) {
		if (!update.writesOnlyStaticColumns()) {
			return;
		}
		final var clustering = update.target()
			.table()
			.getClusteringColumns()
			.keySet()
			.stream()
			.map(ColumnMetadata::getName)
			.collect(Collectors.toSet());
		final var restricted = update.restrictions()
			.stream()
			.flatMap(restriction -> restriction.columns().stream())
			.map(Restriction.Column::name)
			.anyMatch(clustering::contains);
		if (restricted) {
			throw new InvalidQueryException(coordinator, "Invalid restrictions on clustering columns "
				+ "since the UPDATE statement modifies only static columns");
		}
	}

}
