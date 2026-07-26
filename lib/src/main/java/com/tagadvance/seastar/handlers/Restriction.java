package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.tagadvance.seastar.SeaStarRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * One relation of a WHERE clause, resolved against the table it restricts: which column it names,
 * the comparison it applies, and the values it compares against, already converted from terms to
 * the Java values SeaStar stores.
 *
 * <p>Whether a restriction is <em>allowed</em> is the statement's business and stays with the
 * handler - SELECT wants ALLOW FILTERING for a non-key column, UPDATE wants the whole primary key.
 * Whether a restriction <em>matches a row</em> is the same question for every statement, so it is
 * answered once, here.
 *
 * @param columnIndex the position of the restricted column in the table
 * @param column      the name of the restricted column
 * @param operator    the comparison to apply
 * @param values      the values compared against: one for a scalar comparison, one per element for
 *                    IN, none for an operator that takes no term
 */
record Restriction(int columnIndex, CqlIdentifier column, CqlOperator operator, List<Object> values) {

	Restriction {
		// A bound marker with no value resolves to null, so the list has to tolerate one.
		values = Collections.unmodifiableList(new ArrayList<>(values));
	}

	/**
	 * The single value a scalar comparison compares against.
	 */
	Object value() {
		if (values.size() != 1) {
			throw new IllegalStateException(
				"restriction on %s with operator %s carries %d values, not one".formatted(
					column.asInternal(), operator, values.size()));
		}

		return values.get(0);
	}

	/**
	 * The test a row has to pass to satisfy this restriction.
	 *
	 * <p>The remaining operators - the range comparisons, CONTAINS, LIKE, IS NOT NULL - belong here
	 * when they are implemented, so that every statement gains them at once.
	 *
	 * @throws UnsupportedOperationException if SeaStar cannot evaluate this operator yet
	 */
	Predicate<SeaStarRow> toPredicate() {
		return switch (operator) {
			case EQ -> {
				final var expected = value();

				yield row -> Objects.equals(row.getObject(columnIndex), expected);
			}
			case IN -> {
				final Set<Object> expected = new HashSet<>(values);

				yield row -> expected.contains(row.getObject(columnIndex));
			}
			default -> throw new UnsupportedOperationException(
				"Unsupported operator %s in WHERE".formatted(operator));
		};
	}

}
