package com.tagadvance.seastar;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.io.Serializable;
import org.jspecify.annotations.Nullable;

public interface SeaStarRow extends SeaStarReadWriteLock, Row, Serializable {

	SeaStarDriverContext context();

	SeaStarTable table();

	default void set(final String name, final Object value) {
		writeLock(() -> {
			final var index = table().firstIndexOf(name);

			set(index, value);
		});

	}

	default void set(final CqlIdentifier id, Object value) {
		writeLock(() -> {
			final var index = table().firstIndexOf(id);

			set(index, value);
		});
	}

	void set(int i, Object value);

	/**
	 * Opens a slot at {@code i} and fills it with {@code value}, shifting the values after it along.
	 * A row's values are positional, tied to its table's column list, so a column added to the table
	 * has to be added to every row at the same index.
	 *
	 * <p>Called from the table with its write lock already held, which is what keeps the row and the
	 * column list in step.
	 */
	void insertValue(int i, @Nullable Object value);

	/**
	 * Removes the slot at {@code i}, shifting the values after it back. The counterpart of
	 * {@link #insertValue(int, Object)}, for a column dropped from the table.
	 */
	void removeValue(int i);

	default void validate(final int i, final Object value) {
		final var dataType = getColumnDefinitions().get(i).getType();
		final var codec = context().getCodecRegistry().codecFor(dataType);
		// Guava's checkArgument only understands %s; a %d here is left in the message verbatim and
		// pushes every argument into the wrong slot.
		checkArgument(value == null || codec.accepts(value),
			"Value at index %s (%s) is not compatible with column type %s", i, value, dataType);
	}

	Row snapshot();

}
