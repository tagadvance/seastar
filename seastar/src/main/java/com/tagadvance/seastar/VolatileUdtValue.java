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
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * One value of a user defined type.
 *
 * <p>The innermost lock of the hierarchy, and the only {@code Volatile*} class still holding one of
 * its own: a value is not part of the schema tree - it is handed to a caller, who may keep it after
 * the type it was made from has been altered - so there is no keyspace lock that naturally covers
 * it. Nothing is acquired while it is held; whatever the type has to answer is asked for first.
 */
@ThreadSafe
class VolatileUdtValue implements SeaStarUdtValue {

	/**
	 * Immutable; see {@link com.tagadvance.tools.SeaStarReadWriteLock#lock()}.
	 */
	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * Immutable.
	 */
	private final SeaStarUserDefinedType type;
	@GuardedBy("lock")
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

	/**
	 * The value's own lock, the innermost of the hierarchy - nothing else is acquired while it is
	 * held.
	 */
	@Override
	public ReadWriteLock lock() {
		return lock;
	}

	/**
	 * The live type, not a snapshot: an ALTER TYPE performed after this value was created shows
	 * through it, while the value keeps the slots it was created with.
	 */
	@Override
	@NonNull
	public SeaStarUserDefinedType getType() {
		return type;
	}

	@Override
	public int firstIndexOf(final @NonNull String name) {
		return firstIndexOf(CqlIdentifier.fromCql(name));
	}

	/**
	 * Answers from the value's own slots rather than from the type, so a field appended by ALTER
	 * TYPE after this value was created is not found (-1) - every index returned is usable with the
	 * getters.
	 */
	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		return readLockUnchecked(() -> IntStream.range(0, values.size())
			.filter(i -> values.get(i).name().equals(id))
			.findFirst()
			.orElse(-1));
	}

	@Override
	@NonNull
	public List<Integer> allIndicesOf(final @NonNull String name) {
		return allIndicesOf(CqlIdentifier.fromCql(name));
	}

	@Override
	@NonNull
	public List<Integer> allIndicesOf(final @NonNull CqlIdentifier id) {
		// Field names are unique within a type, so the first occurrence is the only one.
		final var index = firstIndexOf(id);
		if (index == -1) {
			throw new IllegalArgumentException("%s is not a field in this UDT".formatted(id));
		}

		return List.of(index);
	}

	/**
	 * The entries, copied out under the read lock so that the encoding and comparison the callers go
	 * on to do happen with no lock held. Encoding asks the type for its attachment point, which takes
	 * the keyspace lock, and that is outside this one.
	 */
	private List<UdtValueEntry> entries() {
		return readLockUnchecked(() -> List.copyOf(values));
	}

	/**
	 * A value written before {@code ALTER TYPE ... ADD} has fewer slots than the type now has.
	 * Reading a missing trailing field as null, rather than throwing, mirrors how a short stored
	 * payload decodes on a cluster; {@code UdtCodec.encode} probes up to the type's field count, not
	 * the value's.
	 */
	@Override
	@Nullable
	public ByteBuffer getBytesUnsafe(final int i) {
		final var entries = entries();

		return i < entries.size() ? entries.get(i).toByteBuffer() : null;
	}

	/**
	 * The field name, type and decoded value are all resolved before the value's own lock is taken.
	 * Reading the type takes the keyspace's lock, which is outside this one; taking them the other
	 * way round would be the one lock inversion this class could still make.
	 */
	@Override
	@NonNull
	public UdtValue setBytesUnsafe(final int i, final ByteBuffer bytes) {
		final var name = type.getFieldNames().get(i);
		final var dataType = type.getFieldTypes().get(i);
		final var decode = codecRegistry().codecFor(dataType).decode(bytes, protocolVersion());
		final var newValue = new UdtValueEntry(name, dataType, decode);

		writeLock(() -> values.set(i, newValue));

		return this;
	}

	/**
	 * The value's slot count, which for a value created before {@code ALTER TYPE ... ADD} is
	 * smaller than the type's field count.
	 */
	@Override
	public int size() {
		return readLockUnchecked(values::size);
	}

	/**
	 * The type the slot was created or last set with. Answers from the value's slots, so an index
	 * the type gained after this value was created throws {@link IndexOutOfBoundsException}.
	 */
	@Override
	@NonNull
	public DataType getType(final int i) {
		return readLockUnchecked(values.get(i)::dataType);
	}

	/**
	 * Resolved through the type's attachment point, which takes the keyspace's read lock - never
	 * call this while holding the value's own lock.
	 */
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

		return IntStream.range(0, entries().size())
			.allMatch(i -> Objects.equals(getObject(i), that.getObject(i)));
	}

	@Override
	public int hashCode() {
		final var fields = IntStream.range(0, entries().size()).mapToObj(this::getObject).toList();

		return Objects.hash(getType(), fields);
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
