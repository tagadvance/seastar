package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.jspecify.annotations.Nullable;

/**
 * One column a statement writes, resolved against the table it writes to: an INSERT value, an
 * UPDATE {@code SET} item, or a column a DELETE clears.
 *
 * <p>Only whole-column assignment is described. The element forms - {@code c = c + v},
 * {@code c[k] = v}, {@code c.f = v} and their deletions - are rejected in {@link Modifications},
 * which is where they belong when they are implemented.
 *
 * @param columnIndex the position of the assigned column in the table
 * @param column      the name of the assigned column
 * @param value       the value to write; null clears the column, which is what a DELETE of a named
 *                    column does
 */
record Assignment(int columnIndex, CqlIdentifier column, @Nullable Object value) {

}
