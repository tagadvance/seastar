package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.jspecify.annotations.NonNull;

public interface SeaStarTable extends SeaStarReadWriteLock, TableMetadata, ColumnDefinitions {

	SeaStarDriverContext context();

	SeaStarKeyspace keyspace();

	default SeaStarColumn addColumn(final @NonNull String name, final @NonNull DataType type) {
		return addColumn(CqlIdentifier.fromInternal(name), type);
	}

	default SeaStarColumn addColumn(final @NonNull CqlIdentifier name,
		final @NonNull DataType type) {
		return addColumn(name, type, false);
	}

	default SeaStarColumn addColumn(final @NonNull CqlIdentifier name, final @NonNull DataType type,
		final boolean isStatic) {
		final var column = new VolatileColumn(context(), this, name, type, isStatic);
		addColumn(column);

		return column;
	}

	void addColumn(final SeaStarColumn column);

	default SeaStarRow addRow(final Object... values) {
		return addRow(List.of(values));
	}

	default SeaStarRow addRow(final List<Object> values) {
		final var row = new VolatileRow(context(), this, values);
		addRow(row);

		return row;
	}

	void addRow(final SeaStarRow row);

	void removeRowIf(Predicate<SeaStarRow> predicate);

	Stream<SeaStarRow> rows();

	void drop();

	void truncate();

	ColumnDefinitions snapshot();

}
