package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
		final Where where;
		try {
			update = Modifications.update(context, getKeyspace, raw, coordinator, bindings);
			validateAssignments(update, coordinator);
			where = resolveWhere(update, coordinator);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var table = update.target().table();
		final var assignments = update.assignments();
		final var conditions = update.conditions();

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			final var matched = table.rows().filter(where.predicate()).toList();

			if (update.ifExists()) {
				if (matched.isEmpty()) {
					return AppliedResultSets.of(context, table, executionInfo, false);
				}
				apply(matched, assignments);

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
				apply(matched, assignments);

				return AppliedResultSets.of(context, table, executionInfo, true);
			}

			if (!matched.isEmpty()) {
				apply(matched, assignments);
			} else if (where.upsertKey() != null) {
				final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
				where.upsertKey().forEach(values::set);
				assignments.forEach(
					assignment -> values.set(assignment.columnIndex(), assignment.value()));
				table.addRow(values);
			}

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	private static void apply(final List<SeaStarRow> matched, final List<Assignment> assignments) {
		for (final var row : matched) {
			for (final var assignment : assignments) {
				row.set(assignment.columnIndex(), assignment.value());
			}
		}
	}

	private static void validateAssignments(final Modification update, final Node coordinator) {
		final var primaryKey = update.target().primaryKeyNames();
		for (final var assignment : update.assignments()) {
			if (primaryKey.contains(assignment.column())) {
				throw new InvalidQueryException(coordinator,
					"PRIMARY KEY part %s found in SET part".formatted(
						assignment.column().asInternal()));
			}
		}
	}

	/**
	 * The rows an UPDATE writes to, plus the primary key of the row it would insert if none match.
	 *
	 * @param upsertKey the value of each primary key column by position, or null when the WHERE
	 *                  clause does not pin the whole primary key to a single row
	 */
	private record Where(Predicate<SeaStarRow> predicate, Map<Integer, Object> upsertKey) {

	}

	private static Where resolveWhere(final Modification update, final Node coordinator) {
		final var primaryKey = update.target().primaryKeyNames();
		final List<Predicate<SeaStarRow>> predicates = new ArrayList<>();
		final Set<CqlIdentifier> restricted = new HashSet<>();
		final Map<Integer, Object> upsertKey = new LinkedHashMap<>();
		for (final var restriction : update.restrictions()) {
			if (!primaryKey.contains(restriction.column())) {
				throw new InvalidQueryException(coordinator,
					"Non PRIMARY KEY column %s found in where clause".formatted(
						restriction.column().asInternal()));
			}
			restricted.add(restriction.column());
			if (restriction.operator() == CqlOperator.EQ) {
				upsertKey.put(restriction.columnIndex(), restriction.value());
			}
			predicates.add(restriction.toPredicate());
		}

		if (!restricted.containsAll(primaryKey)) {
			throw new InvalidQueryException(coordinator,
				"Some primary key parts are missing from the WHERE clause");
		}

		final Predicate<SeaStarRow> predicate = predicates.stream().reduce(Predicate::and)
			.orElseThrow(() -> new IllegalStateException(
				"a WHERE clause with relations must yield at least one predicate"));
		// Only equality on every primary key part can synthesize a row for an upsert.
		final var canUpsert = upsertKey.size() == primaryKey.size();

		return new Where(predicate, canUpsert ? upsertKey : null);
	}

}
