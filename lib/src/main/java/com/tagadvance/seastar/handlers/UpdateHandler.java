package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
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
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.statements.UpdateStatement.ParsedUpdate;
import org.apache.cassandra.utils.Pair;

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
	@SuppressWarnings("unchecked")
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final ParsedUpdate raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final var keyspace = Optional.ofNullable(raw.keyspace())
			.or(() -> getKeyspace.get().map(CqlIdentifier::asInternal))
			.orElse(null);
		if (keyspace == null) {
			throw new InvalidQueryException(coordinator,
				"No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
		}

		final var optionalKeyspace = context.getSeaStarKeyspace(
			CqlIdentifier.fromInternal(keyspace));
		if (optionalKeyspace.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"Keyspace '%s' does not exist".formatted(keyspace)));
		}

		final var optionalTable = optionalKeyspace.get()
			.getSeaStarTable(CqlIdentifier.fromInternal(raw.name()));
		if (optionalTable.isEmpty()) {
			return CompletableFuture.failedStage(new InvalidQueryException(coordinator,
				"table %s does not exist".formatted(raw.name())));
		}
		final var table = optionalTable.get();
		final var codecRegistry = context.getCodecRegistry();
		final var primaryKey = primaryKeyNames(table);

		final List<Object> conditions = Reflections.getDeclaredField(raw, "conditions", List.class)
			.orElseGet(Collections::emptyList);
		if (!conditions.isEmpty()) {
			// TODO: implement LWT (IF <condition>) semantics.
			throw new UnsupportedOperationException(
				"Conditional updates (IF ...) are not currently supported");
		}
		final var ifExists = Reflections.getDeclaredField(raw, "ifExists", Boolean.class)
			.orElse(false);

		final List<Assignment> assignments;
		final Where where;
		try {
			assignments = resolveAssignments(table, primaryKey, raw, codecRegistry, coordinator,
				bindings);
			where = resolveWhere(table, primaryKey, raw, codecRegistry, coordinator, bindings);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		table.writeLock(() -> {
			final var matched = table.rows().filter(where.predicate()).toList();
			if (!matched.isEmpty()) {
				for (final var row : matched) {
					for (final var assignment : assignments) {
						row.set(assignment.index(), assignment.value());
					}
				}
			} else if (!ifExists && where.upsertKey() != null) {
				final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
				where.upsertKey().forEach(values::set);
				for (final var assignment : assignments) {
					values.set(assignment.index(), assignment.value());
				}
				table.addRow(values);
			}
			// TODO: upsert when the primary key is restricted by IN; IF EXISTS should report [applied].
		});

		return CompletableFuture.completedStage(newAsyncResultSet(executionInfo));
	}

	private record Assignment(int index, Object value) {

	}

	@SuppressWarnings("unchecked")
	private static List<Assignment> resolveAssignments(final SeaStarTable table,
		final Set<CqlIdentifier> primaryKey, final ParsedUpdate raw, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		final List<Pair<Object, Object>> updates = Reflections.getDeclaredField(raw, "updates",
			List.class).orElseGet(Collections::emptyList);

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
			final var term = Reflections.getDeclaredField(setValue, "value", Term.Raw.class)
				.orElseThrow();
			final var value = resolveTerm(term, table.get(index).getType(), codecRegistry, bindings);
			assignments.add(new Assignment(index, value));
		}

		return assignments;
	}

	private record Where(Predicate<SeaStarRow> predicate, Map<Integer, Object> upsertKey) {

	}

	private static Where resolveWhere(final SeaStarTable table, final Set<CqlIdentifier> primaryKey,
		final ParsedUpdate raw, final CodecRegistry codecRegistry, final Node coordinator,
		final Object... bindings) {
		final var whereClause = Reflections.getDeclaredField(raw, "whereClause", WhereClause.class)
			.orElseThrow();
		final List<Relation> relations = whereClause.relations;

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
				final var target = resolveTerm(single.getValue(), dataType, codecRegistry, bindings);
				upsertKey.put(index, target);
				predicates.add(row -> Objects.equals(row.getObject(index), target));
			} else if (relation.isIN()) {
				final Set<Object> targets = new HashSet<>();
				for (final var term : single.getInValues()) {
					targets.add(resolveTerm(term, dataType, codecRegistry, bindings));
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
			.orElseThrow();
		// Only equality on every primary key part can synthesize a row for an upsert.
		final var canUpsert = upsertKey.keySet().size() == primaryKey.size();

		return new Where(predicate, canUpsert ? upsertKey : null);
	}

	private static Object resolveTerm(final Term.Raw term, final DataType dataType,
		final CodecRegistry codecRegistry, final Object... bindings) {
		if (term instanceof AbstractMarker.Raw marker) {
			final var bindIndex = Reflections.getDeclaredField(marker, "bindIndex", Integer.class)
				.orElseThrow();

			return bindIndex < bindings.length ? bindings[bindIndex] : null;
		} else if (term instanceof Constants.Literal literal) {
			return codecRegistry.codecFor(dataType).parse(literal.getText());
		}

		throw new UnsupportedOperationException("Unsupported term %s".formatted(term));
	}

	private static Set<CqlIdentifier> primaryKeyNames(final SeaStarTable table) {
		final Set<CqlIdentifier> names = new HashSet<>();
		table.getPartitionKey().stream().map(ColumnMetadata::getName).forEach(names::add);
		table.getClusteringColumns().keySet().stream().map(ColumnMetadata::getName)
			.forEach(names::add);

		return names;
	}

}
