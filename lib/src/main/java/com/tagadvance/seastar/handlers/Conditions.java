package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.utils.Pair;

/**
 * Resolves and evaluates lightweight-transaction {@code IF <col> <op> <value>} conditions against a
 * matched row. Only simple scalar comparisons (=, !=, &lt;, &lt;=, &gt;, &gt;=) are supported.
 */
final class Conditions {

	private Conditions() {
		// hidden constructor
	}

	static List<Condition> translate(final Target target,
		final List<Pair<ColumnIdentifier, ColumnCondition.Raw>> rawConditions,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		final var table = target.table();
		final var primaryKey = target.primaryKeyNames();
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
			final var operator = CqlOperator.of(FieldBindings.CONDITION_OPERATOR.require(condition));
			final var value = Terms.resolve(condition.getValue(), table.get(index).getType(),
				codecRegistry, coordinator, bindings);
			conditions.add(new Condition(index, name, operator, value));
		}

		return List.copyOf(conditions);
	}

	static boolean hold(final List<Condition> conditions, final Row row) {
		return conditions.stream()
			.allMatch(condition -> satisfied(condition, row.getObject(condition.columnIndex())));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static boolean satisfied(final Condition condition, final Object actual) {
		final var operator = condition.operator();
		final var expected = condition.value();
		if (operator == CqlOperator.EQ) {
			return Objects.equals(actual, expected);
		}
		if (operator == CqlOperator.NEQ) {
			return !Objects.equals(actual, expected);
		}
		if (actual == null || expected == null) {
			return false;
		}
		final int comparison = ((Comparable) actual).compareTo(expected);
		if (operator == CqlOperator.LT) {
			return comparison < 0;
		}
		if (operator == CqlOperator.LTE) {
			return comparison <= 0;
		}
		if (operator == CqlOperator.GT) {
			return comparison > 0;
		}
		if (operator == CqlOperator.GTE) {
			return comparison >= 0;
		}

		throw new UnsupportedOperationException("Unsupported IF operator %s".formatted(operator));
	}

	/**
	 * The condition kinds are told apart by which of the mutually exclusive fields is populated, so
	 * absence here is a genuine answer rather than a defaulted lookup.
	 */
	private static void requireScalar(final ColumnCondition.Raw condition) {
		final var unsupported = FieldBindings.CONDITION_COLLECTION_ELEMENT.find(condition).isPresent()
			|| FieldBindings.CONDITION_UDT_FIELD.find(condition).isPresent()
			|| FieldBindings.CONDITION_IN_VALUES.find(condition).isPresent()
			|| FieldBindings.CONDITION_IN_MARKER.find(condition).isPresent();
		if (unsupported) {
			throw new UnsupportedOperationException(
				"Only simple scalar IF conditions are supported");
		}
	}

}
