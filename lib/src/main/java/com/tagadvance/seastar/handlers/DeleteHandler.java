package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
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

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			final var matched = table.rows().filter(predicate).toList();

			if (delete.ifExists()) {
				if (matched.isEmpty()) {
					return AppliedResultSets.of(context, table, executionInfo, false);
				}
				applyDelete(table, matched, deletedColumns, coordinator);

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
				applyDelete(table, matched, deletedColumns, coordinator);

				return AppliedResultSets.of(context, table, executionInfo, true);
			}

			applyDelete(table, matched, deletedColumns, coordinator);

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	/**
	 * A DELETE that names no column removes the whole row; one that names columns clears them and
	 * leaves the row in place.
	 */
	private static void applyDelete(final SeaStarTable table, final List<SeaStarRow> matched,
		final List<Assignment> deletedColumns, final Node coordinator) {
		if (deletedColumns.isEmpty()) {
			table.removeRowIf(matched::contains);
		} else {
			for (final var row : matched) {
				for (final var deleted : deletedColumns) {
					final var index = deleted.columnIndex();
					row.set(index, deleted.apply(row.getObject(index), coordinator));
				}
			}
		}
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
