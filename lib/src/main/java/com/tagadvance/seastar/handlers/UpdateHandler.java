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

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			final var matched = table.rows().filter(predicate).toList();

			if (update.ifExists()) {
				if (matched.isEmpty()) {
					return AppliedResultSets.of(context, table, executionInfo, false);
				}
				apply(matched, assignments, coordinator);

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
				apply(matched, assignments, coordinator);

				return AppliedResultSets.of(context, table, executionInfo, true);
			}

			if (!matched.isEmpty()) {
				apply(matched, assignments, coordinator);
			} else if (upsertKey != null) {
				final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
				upsertKey.forEach(values::set);
				assignments.forEach(assignment -> values.set(assignment.columnIndex(),
					assignment.apply(null, coordinator)));
				table.addRow(values);
			}

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	private static void apply(final List<SeaStarRow> matched, final List<Assignment> assignments,
		final Node coordinator) {
		for (final var row : matched) {
			for (final var assignment : assignments) {
				final var index = assignment.columnIndex();
				row.set(index, assignment.apply(row.getObject(index), coordinator));
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

}
