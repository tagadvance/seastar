package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.WhereClause;
import org.jspecify.annotations.Nullable;

/**
 * Turns the relations of a parsed WHERE clause into {@link Restriction}s against a table. The one
 * place a WHERE clause is read, for every statement that has one.
 */
final class Restrictions {

	private Restrictions() {
		// hidden constructor
	}

	/**
	 * @param where the parsed clause, or {@code null} for a statement written without one
	 */
	static List<Restriction> translate(final SeaStarTable table, final @Nullable WhereClause where,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (where == null) {
			return List.of();
		}

		final List<Restriction> restrictions = new ArrayList<>(where.relations.size());
		for (final var relation : where.relations) {
			// Multi-column and token relations restrict several columns at once, which the model
			// does not describe yet.
			if (!(relation instanceof SingleColumnRelation single)) {
				throw new UnsupportedOperationException("Unsupported relation %s".formatted(relation));
			}
			final var column = CqlIdentifier.fromInternal(single.getEntity().toString());
			final var index = table.firstIndexOf(column);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(column.asInternal()));
			}
			final var operator = CqlOperator.of(relation.operator());
			final var values = values(single, operator, table.get(index).getType(), codecRegistry,
				coordinator, bindings);
			restrictions.add(new Restriction(index, column, operator, values));
		}

		return List.copyOf(restrictions);
	}

	private static List<Object> values(final SingleColumnRelation relation,
		final CqlOperator operator, final DataType dataType, final CodecRegistry codecRegistry,
		final Node coordinator, final Object... bindings) {
		if (operator == CqlOperator.IN) {
			final var inValues = relation.getInValues();
			if (inValues == null) {
				// `col IN ?` binds the whole list to a single marker, which SeaStar does not model.
				throw new UnsupportedOperationException(
					"Unsupported relation %s".formatted(relation));
			}
			final List<Object> values = new ArrayList<>(inValues.size());
			for (final var term : inValues) {
				values.add(Terms.resolve(term, dataType, codecRegistry, coordinator, bindings));
			}

			return values;
		}

		// IS NOT NULL carries no term at all.
		final var value = relation.getValue();

		return value == null ? List.of()
			: Collections.singletonList(
				Terms.resolve(value, dataType, codecRegistry, coordinator, bindings));
	}

}
