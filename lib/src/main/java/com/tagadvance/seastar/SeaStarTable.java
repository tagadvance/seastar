package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.datastax.oss.driver.internal.core.data.ValuesHelper;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface SeaStarTable extends TableMetadata, ColumnDefinitions {

	SeaStarDriverContext context();

	void truncate();

	default Row addRow(final Object... values) {
		// TODO: validate values
		final var spliterator = Spliterators.spliteratorUnknownSize(iterator(),
			Spliterator.ORDERED);
		final var types = StreamSupport.stream(spliterator, false)
			.map(ColumnDefinition::getType)
			.toList();

		final var byteBuffers = ValuesHelper.encodeValues(values, types,
			context().getCodecRegistry(), context().getProtocolVersion());

		return addRow(List.of(byteBuffers));
	}

	default Row addRow(final List<ByteBuffer> byteBuffers) {
		// TODO: validate byte buffers
		final var row = new DefaultRow(this, byteBuffers, context());
		addRow(row);

		return row;
	}

	void addRow(Row row);

	void removeRowIf(Predicate<Row> predicate);

	Stream<Row> rows();

	void detach();

}
