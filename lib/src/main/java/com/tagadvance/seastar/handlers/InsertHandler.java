package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedInsert;

@ThreadSafe
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

		final Modification insert;
		try {
			insert = Modifications.insert(context, getKeyspace, raw, coordinator, bindings);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var target = insert.target();
		final var table = target.table();
		final var assignments = insert.assignments();

		final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
		assignments.forEach(assignment -> values.set(assignment.columnIndex(), assignment.value()));

		final var primaryKey = target.primaryKeyNames();
		final var named = assignments.stream().map(Assignment::column).collect(Collectors.toSet());
		for (final var part : primaryKey) {
			if (!named.contains(part)) {
				return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
					"Missing mandatory PRIMARY KEY part %s".formatted(part.asInternal())));
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
			final var existing = table.rows().filter(samePrimaryKey).findFirst().orElse(null);
			if (insert.ifNotExists()) {
				if (existing == null) {
					table.addRow(values);

					return AppliedResultSets.of(context, table, executionInfo, true);
				}

				return AppliedResultSets.ofExisting(context, table, executionInfo,
					existing.snapshot());
			}
			// INSERT is an upsert; write only the named columns, preserving any columns this
			// statement did not specify on a row that already shares this primary key. An
			// explicitly-inserted NULL still clears its column; an unnamed column is left as-is.
			if (existing == null) {
				table.addRow(values);
			} else {
				assignments.forEach(
					assignment -> existing.set(assignment.columnIndex(), assignment.value()));
			}

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

}
