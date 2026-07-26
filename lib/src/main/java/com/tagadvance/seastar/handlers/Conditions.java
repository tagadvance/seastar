package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.utils.Pair;

/**
 * Resolves and evaluates lightweight-transaction {@code IF <col> <op> <value>} conditions against a
 * matched row. Only simple scalar comparisons (=, !=, &lt;, &lt;=, &gt;, &gt;=) are supported.
 */
final class Conditions {

	private Conditions() {
	}

	record Condition(int index, Operator operator, Object value) {

	}

	static List<Condition> resolve(final SeaStarTable table, final Set<CqlIdentifier> primaryKey,
		final List<Pair<Object, Object>> rawConditions, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		final List<Condition> conditions = new ArrayList<>(rawConditions.size());
		for (final var raw : rawConditions) {
			final var name = CqlIdentifier.fromInternal(raw.left.toString());
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(name.asInternal()));
			}
			if (primaryKey.contains(name)) {
				throw new InvalidQueryException(coordinator,
					"PRIMARY KEY column '%s' cannot have IF conditions".formatted(name.asInternal()));
			}

			final var condition = raw.right;
			requireScalar(condition);
			final var operator = Reflections.getDeclaredField(condition, "operator", Operator.class)
				.orElseThrow();
			final var term = Reflections.getDeclaredField(condition, "value", Term.Raw.class)
				.orElseThrow();
			final var value = Terms.resolve(term, table.get(index).getType(), codecRegistry,
				coordinator, bindings);
			conditions.add(new Condition(index, operator, value));
		}

		return conditions;
	}

	static boolean hold(final List<Condition> conditions, final Row row) {
		return conditions.stream()
			.noneMatch(condition -> !satisfied(condition, row.getObject(condition.index())));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static boolean satisfied(final Condition condition, final Object actual) {
		final var operator = condition.operator();
		final var expected = condition.value();
		if (operator == Operator.EQ) {
			return Objects.equals(actual, expected);
		}
		if (operator == Operator.NEQ) {
			return !Objects.equals(actual, expected);
		}
		if (actual == null || expected == null) {
			return false;
		}
		final int comparison = ((Comparable) actual).compareTo(expected);
		if (operator == Operator.LT) {
			return comparison < 0;
		}
		if (operator == Operator.LTE) {
			return comparison <= 0;
		}
		if (operator == Operator.GT) {
			return comparison > 0;
		}
		if (operator == Operator.GTE) {
			return comparison >= 0;
		}

		throw new UnsupportedOperationException("Unsupported IF operator %s".formatted(operator));
	}

	private static void requireScalar(final Object condition) {
		final var unsupported = Reflections.getDeclaredField(condition, "collectionElement",
			Term.Raw.class).isPresent()
			|| Reflections.getDeclaredField(condition, "udtField", Object.class).isPresent()
			|| Reflections.getDeclaredField(condition, "inValues", List.class).isPresent()
			|| Reflections.getDeclaredField(condition, "inMarker", Object.class).isPresent();
		if (unsupported) {
			throw new UnsupportedOperationException(
				"Only simple scalar IF conditions are supported");
		}
	}

}
