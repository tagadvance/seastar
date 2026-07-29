package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.jspecify.annotations.Nullable;

/**
 * One {@code IF <column> <operator> <value>} condition of a lightweight transaction, resolved
 * against the table it tests.
 *
 * @param columnIndex the position of the tested column in the table
 * @param column      the name of the tested column
 * @param operator    the comparison to apply
 * @param value       the value compared against
 */
record Condition(int columnIndex, CqlIdentifier column, CqlOperator operator,
				 @Nullable Object value) {

}
