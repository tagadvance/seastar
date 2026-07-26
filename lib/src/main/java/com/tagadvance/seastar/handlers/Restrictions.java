package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.MultiColumnRelation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
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
			if (relation instanceof SingleColumnRelation single) {
				restrictions.add(single(table, single, codecRegistry, coordinator, bindings));
			} else if (relation instanceof MultiColumnRelation multi) {
				restrictions.add(multi(table, multi, codecRegistry, coordinator, bindings));
			} else {
				// A token() relation restricts the partition token rather than a column, which the
				// model does not describe.
				throw new UnsupportedOperationException("Unsupported relation %s".formatted(relation));
			}
		}

		return List.copyOf(restrictions);
	}

	private static Restriction single(final SeaStarTable table, final SingleColumnRelation relation,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		final var column = column(table, relation.getEntity(), coordinator);
		final var operator = CqlOperator.of(relation.operator());
		final var termType = termType(column.type(), operator);

		if (operator == CqlOperator.IN) {
			final var inValues = relation.getInValues();
			if (inValues == null) {
				// `col IN ?` binds the whole list to a single marker, which SeaStar does not model.
				throw new UnsupportedOperationException(
					"Unsupported relation %s".formatted(relation));
			}
			final List<List<Object>> values = new ArrayList<>(inValues.size());
			for (final var term : inValues) {
				values.add(Collections.singletonList(
					Terms.resolve(term, termType, codecRegistry, coordinator, bindings)));
			}

			return new Restriction(List.of(column), operator, values);
		}

		// IS NOT NULL carries no term at all.
		final var value = relation.getValue();
		final List<List<Object>> values = value == null ? List.of()
			: List.of(Collections.singletonList(
				Terms.resolve(value, termType, codecRegistry, coordinator, bindings)));

		return new Restriction(List.of(column), operator, values);
	}

	private static Restriction multi(final SeaStarTable table, final MultiColumnRelation relation,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		final var columns = relation.getEntities()
			.stream()
			.map(entity -> column(table, entity, coordinator))
			.toList();
		final var operator = CqlOperator.of(relation.operator());

		if (operator == CqlOperator.IN) {
			final var inValues = relation.getInValues();
			if (inValues == null) {
				throw new UnsupportedOperationException(
					"Unsupported relation %s".formatted(relation));
			}
			final List<List<Object>> values = new ArrayList<>(inValues.size());
			for (final var term : inValues) {
				values.add(tuple(term, columns, codecRegistry, coordinator, bindings));
			}

			return new Restriction(columns, operator, values);
		}

		return new Restriction(columns, operator,
			List.of(tuple(relation.getValue(), columns, codecRegistry, coordinator, bindings)));
	}

	/**
	 * The components of a multi-column relation's value, each resolved against the type of the
	 * column it is compared with rather than against a tuple type: {@code (a, b) > (1, 'x')} names
	 * two independent columns, not a tuple column.
	 */
	private static List<Object> tuple(final Term.Raw term, final List<Restriction.Column> columns,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (!(term instanceof Tuples.Literal literal)) {
			// `(a, b) > ?` binds the whole tuple to one marker, which SeaStar does not model.
			throw new UnsupportedOperationException("Unsupported relation value %s".formatted(term));
		}

		final var elements = FieldBindings.TUPLE_ELEMENTS.require(literal);
		if (elements.size() != columns.size()) {
			throw new InvalidQueryException(coordinator,
				"Expected %d elements in value tuple, but got %d: %s".formatted(columns.size(),
					elements.size(), term.getText()));
		}

		final List<Object> values = new ArrayList<>(elements.size());
		for (int i = 0; i < elements.size(); i++) {
			values.add(Terms.resolve(elements.get(i), columns.get(i).type(), codecRegistry,
				coordinator, bindings));
		}

		return Collections.unmodifiableList(values);
	}

	/**
	 * CONTAINS compares against what a collection holds rather than against the collection, so its
	 * term resolves as an element, a value or a key. A column that holds no collection at all is
	 * {@link RestrictionRules}' to reject, so the column's own type stands in here.
	 */
	private static DataType termType(final DataType columnType, final CqlOperator operator) {
		if (operator == CqlOperator.CONTAINS) {
			if (columnType instanceof ListType list) {
				return list.getElementType();
			}
			if (columnType instanceof SetType set) {
				return set.getElementType();
			}
			if (columnType instanceof MapType map) {
				return map.getValueType();
			}
		}
		if (operator == CqlOperator.CONTAINS_KEY && columnType instanceof MapType map) {
			return map.getKeyType();
		}

		return columnType;
	}

	private static Restriction.Column column(final SeaStarTable table,
		final ColumnIdentifier entity, final Node coordinator) {
		final var name = CqlIdentifier.fromInternal(entity.toString());
		final var index = table.firstIndexOf(name);
		if (index < 0) {
			throw new InvalidQueryException(coordinator,
				"Undefined column name %s".formatted(name.asInternal()));
		}

		return new Restriction.Column(index, name, table.get(index).getType());
	}

}
