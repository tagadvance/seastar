package com.tagadvance.seastar;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.io.Serializable;

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

	default void validate(final int i, final Object value) {
		final var dataType = getColumnDefinitions().get(i).getType();
		final var codec = context().getCodecRegistry().codecFor(dataType);
		checkArgument(codec.accepts(value), "Value %d (%s) is not compatible with column type %s",
			i, value, dataType);
	}

	Row snapshot();

}
