package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A result row over values a SELECT clause computed rather than values a stored row holds. An
 * aggregate, a {@code writetime}, a {@code token} and a {@code SELECT JSON} all produce a column
 * that exists nowhere in the store, so the row that carries them is built here.
 */
final class ValueRow implements Row {

	private final ColumnDefinitions definitions;
	private final List<Object> values;
	private final CodecRegistry codecRegistry;
	private final ProtocolVersion version;

	ValueRow(final ColumnDefinitions definitions, final List<Object> values,
		final CodecRegistry codecRegistry, final ProtocolVersion version) {
		this.definitions = definitions;
		this.values = values;
		this.codecRegistry = codecRegistry;
		this.version = version;
	}

	@Override
	public boolean isDetached() {
		return false;
	}

	@Override
	public void attach(final @NonNull AttachmentPoint attachmentPoint) {
		throw new UnsupportedOperationException();
	}

	@Override
	@NonNull
	public CodecRegistry codecRegistry() {
		return codecRegistry;
	}

	@Override
	@NonNull
	public ProtocolVersion protocolVersion() {
		return version;
	}

	@Override
	public int size() {
		return definitions.size();
	}

	@Override
	@NonNull
	public DataType getType(final int i) {
		return definitions.get(i).getType();
	}

	@Override
	public @Nullable ByteBuffer getBytesUnsafe(final int i) {
		final TypeCodec<Object> codec = codecRegistry.codecFor(getType(i));

		return codec.encode(values.get(i), version);
	}

	@Override
	public int firstIndexOf(final @NonNull String name) {
		return definitions.firstIndexOf(name);
	}

	@Override
	@NonNull
	public DataType getType(final @NonNull String name) {
		return definitions.get(name).getType();
	}

	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		return definitions.firstIndexOf(id);
	}

	@Override
	@NonNull
	public DataType getType(final @NonNull CqlIdentifier id) {
		return definitions.get(id).getType();
	}

	@Override
	@NonNull
	public ColumnDefinitions getColumnDefinitions() {
		return definitions;
	}

}
