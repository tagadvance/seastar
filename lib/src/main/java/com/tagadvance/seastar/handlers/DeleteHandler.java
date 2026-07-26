package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
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

		final Target target;
		try {
			target = Targets.require(context, getKeyspace, raw, coordinator);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}
		final var table = target.table();
		final var codecRegistry = context.getCodecRegistry();
		final var primaryKey = target.primaryKeyNames();

		final var conditionList = raw.getConditions();
		final var ifExists = FieldBindings.MODIFICATION_IF_EXISTS.require(raw);

		final int[] deletedColumns;
		final Predicate<SeaStarRow> predicate;
		final List<Conditions.Condition> conditions;
		try {
			deletedColumns = resolveDeletedColumns(table, primaryKey, raw, coordinator);
			predicate = resolveWhere(table, primaryKey, raw, codecRegistry, coordinator, bindings);
			conditions = Conditions.resolve(table, primaryKey, conditionList, codecRegistry,
				coordinator, bindings);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			final var matched = table.rows().filter(predicate).toList();

			if (ifExists) {
				if (matched.isEmpty()) {
					return AppliedResultSets.of(context, table, executionInfo, false);
				}
				applyDelete(table, matched, deletedColumns);
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
				applyDelete(table, matched, deletedColumns);
				return AppliedResultSets.of(context, table, executionInfo, true);
			}

			applyDelete(table, matched, deletedColumns);

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	private static void applyDelete(final SeaStarTable table, final List<SeaStarRow> matched,
		final int[] deletedColumns) {
		if (deletedColumns.length == 0) {
			table.removeRowIf(matched::contains);
		} else {
			for (final var row : matched) {
				for (final var index : deletedColumns) {
					row.set(index, null);
				}
			}
		}
	}

	private static int[] resolveDeletedColumns(final SeaStarTable table,
		final Set<CqlIdentifier> primaryKey, final Parsed raw, final Node coordinator) {
		final var deletions = FieldBindings.DELETE_DELETIONS.require(raw);

		final var indices = new int[deletions.size()];
		for (int i = 0; i < deletions.size(); i++) {
			final var name = CqlIdentifier.fromInternal(deletions.get(i).affectedColumn().toString());
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(name.asInternal()));
			}
			if (primaryKey.contains(name)) {
				throw new InvalidQueryException(coordinator,
					"Invalid identifier %s for deletion (should not be a PRIMARY KEY part)".formatted(
						name.asInternal()));
			}
			indices[i] = index;
		}

		return indices;
	}

	private static Predicate<SeaStarRow> resolveWhere(final SeaStarTable table,
		final Set<CqlIdentifier> primaryKey, final Parsed raw, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		final List<Relation> relations = FieldBindings.DELETE_WHERE_CLAUSE.require(raw).relations;

		final List<Predicate<SeaStarRow>> predicates = new ArrayList<>();
		final Set<CqlIdentifier> restricted = new HashSet<>();
		for (final var relation : relations) {
			if (!(relation instanceof SingleColumnRelation single)) {
				throw new UnsupportedOperationException("Unsupported relation %s".formatted(relation));
			}
			final var name = CqlIdentifier.fromInternal(single.getEntity().toString());
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(name.asInternal()));
			}
			if (!primaryKey.contains(name)) {
				throw new InvalidQueryException(coordinator,
					"Non PRIMARY KEY column %s found in where clause".formatted(name.asInternal()));
			}
			restricted.add(name);

			final var dataType = table.get(index).getType();
			if (relation.isEQ()) {
				final var target = Terms.resolve(single.getValue(), dataType, codecRegistry,
					coordinator, bindings);
				predicates.add(row -> Objects.equals(row.getObject(index), target));
			} else if (relation.isIN()) {
				final Set<Object> targets = new HashSet<>();
				for (final var term : single.getInValues()) {
					targets.add(Terms.resolve(term, dataType, codecRegistry, coordinator, bindings));
				}
				predicates.add(row -> targets.contains(row.getObject(index)));
			} else {
				throw new UnsupportedOperationException(
					"Unsupported operator %s in WHERE".formatted(relation.operator()));
			}
		}

		if (!restricted.containsAll(primaryKey)) {
			throw new InvalidQueryException(coordinator,
				"Some partition key parts are missing from the WHERE clause");
		}

		return predicates.stream().reduce(Predicate::and)
			.orElseThrow(() -> new IllegalStateException(
				"a WHERE clause with relations must yield at least one predicate"));
	}

}
