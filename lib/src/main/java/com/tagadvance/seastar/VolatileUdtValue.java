package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.data.ValuesHelper;
import com.tagadvance.tools.Locks;
import com.tagadvance.tools.Locks.SeaStarReadWriteLock;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

@ThreadSafe
public class VolatileUdtValue implements SeaStarUdtValue {

	private final SeaStarReadWriteLock lock = Locks.newLock();

	private final SeaStarUserDefinedType type;
	private final List<UdtValueEntry> values = new ArrayList<>();

	public VolatileUdtValue(@NonNull SeaStarUserDefinedType type) {
		this(type, new ByteBuffer[type.getFieldTypes().size()]);
	}

	public VolatileUdtValue(@NonNull SeaStarUserDefinedType type, @NonNull Object... values) {
		this(type, ValuesHelper.encodeValues(values, type.getFieldTypes(),
			type.getAttachmentPoint().getCodecRegistry(),
			type.getAttachmentPoint().getProtocolVersion()));
	}

	private VolatileUdtValue(final SeaStarUserDefinedType type, final ByteBuffer[] values) {
		this.type = requireNonNull(type, "type must not be null");
		requireNonNull(values, "values must not be null");

		final var fieldNames = type.getFieldNames();
		final var fieldTypes = type.getFieldTypes();
		if (fieldNames.size() != fieldTypes.size() || fieldTypes.size() != values.length) {
			throw new IllegalArgumentException(
				"fieldNames, fieldTypes, and values must have the same length");
		}

		lock.writeLock(() -> IntStream.range(0, values.length)
			.mapToObj(i -> new UdtValueEntry(fieldNames.get(i), fieldTypes.get(i), values[i]))
			.forEach(this.values::add));
	}

	@Override
	@NonNull
	public UserDefinedType getType() {
		return type;
	}

	@Override
	public int firstIndexOf(final @NonNull String name) {
		return firstIndexOf(CqlIdentifier.fromCql(name));
	}

	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		return lock.readLockUnchecked(() -> IntStream.range(0, values.size())
			.filter(i -> values.get(i).name().equals(id))
			.findFirst()
			.orElse(-1));
	}

	@Override
	@Nullable
	public ByteBuffer getBytesUnsafe(final int i) {
		return lock.readLockUnchecked(values.get(i)::value);
	}

	@Override
	@NonNull
	public UdtValue setBytesUnsafe(final int i, final ByteBuffer v) {
		lock.writeLock(() -> {
			final var value = values.get(i);
			final var newValue = new UdtValueEntry(value.name, value.dataType, v);

			values.set(i, newValue);
		});

		return this;
	}

	@Override
	public int size() {
		return lock.readLockUnchecked(values::size);
	}

	@Override
	@NonNull
	public DataType getType(final int i) {
		return lock.readLockUnchecked(values.get(i)::dataType);
	}

	@Override
	@NonNull
	public CodecRegistry codecRegistry() {
		return type.getAttachmentPoint().getCodecRegistry();
	}

	@Override
	@NonNull
	public ProtocolVersion protocolVersion() {
		return ProtocolVersion.DEFAULT;
	}

	private record UdtValueEntry(@NonNull CqlIdentifier name, @NonNull DataType dataType,
								 @Nullable ByteBuffer value) {

		private UdtValueEntry {
			requireNonNull(name, "name must not be null");
			requireNonNull(dataType, "dataType must not be null");
		}

	}

}
