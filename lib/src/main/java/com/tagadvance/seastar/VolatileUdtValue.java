package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

@ThreadSafe
public class VolatileUdtValue implements SeaStarUdtValue {

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final SeaStarUserDefinedType type;
	private final List<UdtValueEntry> values = new ArrayList<>();

	public VolatileUdtValue(@NonNull SeaStarUserDefinedType type, @NonNull Object... values) {
		this(type, List.of(values));
	}

	private VolatileUdtValue(final SeaStarUserDefinedType type, final List<Object> values) {
		this.type = requireNonNull(type, "type must not be null");
		type.readLock(() -> {
			validate(values);

			final var fieldNames = type.getFieldNames();
			final var fieldTypes = type.getFieldTypes();

			// Create one slot per field, filling the leading slots with the provided values and
			// leaving the rest unset (null), mirroring UserDefinedType.newValue(Object...).
			writeLock(() -> IntStream.range(0, fieldNames.size())
				.mapToObj(i -> new UdtValueEntry(fieldNames.get(i), fieldTypes.get(i),
					i < values.size() ? values.get(i) : null))
				.forEach(this.values::add));
		});
	}

	@Override
	public ReadWriteLock lock() {
		return lock;
	}

	@Override
	@NonNull
	public SeaStarUserDefinedType getType() {
		return type;
	}

	@Override
	public int firstIndexOf(final @NonNull String name) {
		return firstIndexOf(CqlIdentifier.fromCql(name));
	}

	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		return readLockUnchecked(() -> IntStream.range(0, values.size())
			.filter(i -> values.get(i).name().equals(id))
			.findFirst()
			.orElse(-1));
	}

	@Override
	@Nullable
	public ByteBuffer getBytesUnsafe(final int i) {
		// A value written before ALTER TYPE ... ADD has fewer slots than the type now has. Reading the
		// missing trailing fields as null mirrors how a short stored payload decodes on a cluster;
		// UdtCodec.encode probes up to the type's field count, not the value's.
		return readLockUnchecked(() -> i < values.size() ? values.get(i).toByteBuffer() : null);
	}

	@Override
	@NonNull
	public UdtValue setBytesUnsafe(final int i, final ByteBuffer bytes) {
		writeLock(() -> {
			final var name = type.getFieldNames().get(i);
			final var dataType = type.getFieldTypes().get(i);
			final var decode = codecRegistry().codecFor(dataType).decode(bytes, protocolVersion());
			final var newValue = new UdtValueEntry(name, dataType, decode);

			values.set(i, newValue);
		});

		return this;
	}

	@Override
	public int size() {
		return readLockUnchecked(values::size);
	}

	@Override
	@NonNull
	public DataType getType(final int i) {
		return readLockUnchecked(values.get(i)::dataType);
	}

	@Override
	@NonNull
	public CodecRegistry codecRegistry() {
		return type.getAttachmentPoint().getCodecRegistry();
	}

	@Override
	@NonNull
	public ProtocolVersion protocolVersion() {
		return type.context().getProtocolVersion();
	}

	// Value equality, matching DefaultUdtValue. Without it a UDT read out of a row could never equal
	// one built from a literal, so WHERE and IF comparisons on a UDT column would never match.
	@Override
	public boolean equals(final Object other) {
		if (other == this) {
			return true;
		}
		if (!(other instanceof UdtValue that)) {
			return false;
		}
		if (!getType().equals(that.getType())) {
			return false;
		}

		return readLockUnchecked(() -> IntStream.range(0, values.size())
			.allMatch(i -> Objects.equals(getObject(i), that.getObject(i))));
	}

	@Override
	public int hashCode() {
		return readLockUnchecked(() -> {
			final var fields = IntStream.range(0, values.size()).mapToObj(this::getObject).toList();

			return Objects.hash(getType(), fields);
		});
	}

	@Immutable
	private class UdtValueEntry {

		private final CqlIdentifier name;
		private final DataType dataType;
		private final Object value;

		private UdtValueEntry(@NonNull CqlIdentifier name, @NonNull DataType dataType,
			@Nullable Object value) {
			this.name = requireNonNull(name, "name must not be null");
			this.dataType = requireNonNull(dataType, "dataType must not be null");
			this.value = value;
		}

		public CqlIdentifier name() {
			return name;
		}

		public DataType dataType() {
			return dataType;
		}

		public Object value() {
			return value;
		}

		public ByteBuffer toByteBuffer() {
			return codecRegistry().codecFor(dataType).encode(value, protocolVersion());
		}

	}

}
