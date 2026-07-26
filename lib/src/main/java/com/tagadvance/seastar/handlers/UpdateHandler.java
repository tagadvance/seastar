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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
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

		final List<Assignment> assignments;
		final Where where;
		final List<Conditions.Condition> conditions;
		try {
			assignments = resolveAssignments(table, primaryKey, raw, codecRegistry, coordinator,
				bindings);
			where = resolveWhere(table, primaryKey, raw, codecRegistry, coordinator, bindings);
			conditions = Conditions.resolve(table, primaryKey, conditionList, codecRegistry,
				coordinator, bindings);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final AsyncResultSet result = table.writeLockUnchecked(() -> {
			final var matched = table.rows().filter(where.predicate()).toList();

			if (ifExists) {
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
				for (final var assignment : assignments) {
					values.set(assignment.index(), assignment.value());
				}
				table.addRow(values);
			}

			return newAsyncResultSet(executionInfo);
		});

		return CompletableFuture.completedStage(result);
	}

	private static void apply(final List<SeaStarRow> matched, final List<Assignment> assignments) {
		for (final var row : matched) {
			for (final var assignment : assignments) {
				row.set(assignment.index(), assignment.value());
			}
		}
	}

	private record Assignment(int index, Object value) {

	}

	private static List<Assignment> resolveAssignments(final SeaStarTable table,
		final Set<CqlIdentifier> primaryKey, final ParsedUpdate raw, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		final var updates = FieldBindings.UPDATE_UPDATES.require(raw);

		final List<Assignment> assignments = new ArrayList<>(updates.size());
		for (final var update : updates) {
			final var name = CqlIdentifier.fromInternal(update.left.toString());
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(name.asInternal()));
			}
			if (primaryKey.contains(name)) {
				throw new InvalidQueryException(coordinator,
					"PRIMARY KEY part %s found in SET part".formatted(name.asInternal()));
			}
			if (!(update.right instanceof Operation.SetValue setValue)) {
				throw new UnsupportedOperationException(
					"Unsupported UPDATE assignment %s".formatted(update.right));
			}
			final var term = FieldBindings.SET_VALUE.require(setValue);
			final var value = Terms.resolve(term, table.get(index).getType(), codecRegistry,
				coordinator, bindings);
			assignments.add(new Assignment(index, value));
		}

		return assignments;
	}

	private record Where(Predicate<SeaStarRow> predicate, Map<Integer, Object> upsertKey) {

	}

	private static Where resolveWhere(final SeaStarTable table, final Set<CqlIdentifier> primaryKey,
		final ParsedUpdate raw, final CodecRegistry codecRegistry, final Node coordinator,
		final Object... bindings) {
		final List<Relation> relations = FieldBindings.UPDATE_WHERE_CLAUSE.require(raw).relations;

		final List<Predicate<SeaStarRow>> predicates = new ArrayList<>();
		final Set<CqlIdentifier> restricted = new HashSet<>();
		final Map<Integer, Object> upsertKey = new LinkedHashMap<>();
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
				upsertKey.put(index, target);
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
				"Some primary key parts are missing from the WHERE clause");
		}

		final Predicate<SeaStarRow> predicate = predicates.stream().reduce(Predicate::and)
			.orElseThrow(() -> new IllegalStateException(
				"a WHERE clause with relations must yield at least one predicate"));
		// Only equality on every primary key part can synthesize a row for an upsert.
		final var canUpsert = upsertKey.keySet().size() == primaryKey.size();

		return new Where(predicate, canUpsert ? upsertKey : null);
	}

}
