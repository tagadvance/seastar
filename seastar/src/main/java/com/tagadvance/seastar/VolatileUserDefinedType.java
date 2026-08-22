package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.IntStream;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * A user defined type. Holds no lock of its own; a type belongs to a keyspace and is guarded by that
 * keyspace's lock, the same one its tables are. See the lock hierarchy in {@code AGENTS.md}.
 */
@ThreadSafe
public class VolatileUserDefinedType implements SeaStarUserDefinedType {

	/**
	 * Immutable.
	 */
	private final SeaStarDriverContext context;
	/**
	 * Immutable, and the owner of the lock every field below is guarded by.
	 */
	private final SeaStarKeyspace keyspace;
	/**
	 * Immutable.
	 */
	private final CqlIdentifier name;
	/**
	 * Immutable.
	 */
	private final boolean isFrozen;
	@GuardedBy("keyspace.lock()")
	private final List<UserDefinedTypeDefinition> definitions;
	@GuardedBy("keyspace.lock()")
	private AttachmentPoint attachmentPoint;

	public VolatileUserDefinedType(final SeaStarDriverContext context, final SeaStarKeyspace keyspace,
		final CqlIdentifier name, final boolean isFrozen,
		final List<UserDefinedTypeDefinition> definitions) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace = requireNonNull(keyspace, "keyspace must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.isFrozen = isFrozen;
		this.definitions = new ArrayList<>(
			requireNonNull(definitions, "definitions must not be null"));
		this.attachmentPoint = context;
	}

	/**
	 * A copy that differs only in whether it is frozen, sharing the original's field list so that
	 * ALTER TYPE stays visible through every variant handed out as a column type.
	 */
	private VolatileUserDefinedType(final VolatileUserDefinedType original, final boolean isFrozen) {
		this.context = original.context;
		this.keyspace = original.keyspace;
		this.name = original.name;
		this.isFrozen = isFrozen;
		this.definitions = original.definitions;
		this.attachmentPoint = original.context;
	}

	@Override
	public ReadWriteLock lock() {
		return keyspace.lock();
	}

	@Override
	public SeaStarDriverContext context() {
		return context;
	}

	@Override
	public CqlIdentifier getKeyspace() {
		return keyspace.name();
	}

	@Override
	@NonNull
	public CqlIdentifier getName() {
		return name;
	}

	@Override
	public boolean isFrozen() {
		return isFrozen;
	}

	@Override
	@NonNull
	public List<CqlIdentifier> getFieldNames() {
		return readLockUnchecked(
			() -> definitions.stream().map(UserDefinedTypeDefinition::name).toList());
	}

	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		return readLockUnchecked(() -> IntStream.range(0, definitions.size())
			.filter(i -> definitions.get(i).name().equals(id))
			.findFirst()
			.orElse(-1));
	}

	@Override
	public int firstIndexOf(final @NonNull String name) {
		return firstIndexOf(CqlIdentifier.fromCql(name));
	}

	@Override
	@NonNull
	public List<Integer> allIndicesOf(final @NonNull CqlIdentifier id) {
		// Field names are unique within a type, so the first occurrence is the only one. A missing
		// field is an empty list rather than a throw, matching DefaultUserDefinedType.
		final var index = firstIndexOf(id);

		return index == -1 ? List.of() : List.of(index);
	}

	@Override
	@NonNull
	public List<Integer> allIndicesOf(final @NonNull String name) {
		return allIndicesOf(CqlIdentifier.fromCql(name));
	}

	@Override
	@NonNull
	public List<DataType> getFieldTypes() {
		return readLockUnchecked(
			() -> definitions.stream().map(UserDefinedTypeDefinition::dataType).toList());
	}

	// Copies share the definitions so that ALTER TYPE stays visible through every frozen and
	// non-frozen variant handed out as a column type, the way schema propagates on a cluster. They
	// share a lock too, because they share a keyspace.
	@Override
	@NonNull
	public UserDefinedType copy(final boolean newFrozen) {
		return new VolatileUserDefinedType(this, newFrozen);
	}

	@Override
	public void addField(final CqlIdentifier name, final DataType dataType) {
		writeLock(() -> definitions.add(new UserDefinedTypeDefinition(name, dataType)));
	}

	@Override
	public void renameFields(final Map<CqlIdentifier, CqlIdentifier> renames) {
		writeLock(() -> {
			final var renamed = definitions.stream()
				.map(definition -> new UserDefinedTypeDefinition(
					renames.getOrDefault(definition.name(), definition.name()), definition.dataType()))
				.toList();
			definitions.clear();
			definitions.addAll(renamed);
		});
	}

	@Override
	@NonNull
	public SeaStarUdtValue newValue() {
		return readLockUnchecked(() -> new VolatileUdtValue(this));
	}

	@Override
	@NonNull
	public UdtValue newValue(final Object @NonNull ... values) {
		return readLockUnchecked(() -> new VolatileUdtValue(this, values));
	}

	@Override
	@NonNull
	public AttachmentPoint getAttachmentPoint() {
		return readLockUnchecked(() -> attachmentPoint);
	}

	@Override
	public boolean isDetached() {
		return readLockUnchecked(() -> attachmentPoint == AttachmentPoint.NONE);
	}

	@Override
	public void attach(final @NonNull AttachmentPoint attachmentPoint) {
		writeLock(() -> {
			this.attachmentPoint = requireNonNull(attachmentPoint,
				"attachmentPoint must not be null");
			getFieldTypes().forEach(type -> type.attach(attachmentPoint));
		});
	}

	@Override
	public boolean equals(final Object other) {
		if (other == this) {
			return true;
		} else if (other instanceof UserDefinedType that) {
			// frozen is ignored in comparisons, matching DefaultUserDefinedType. This lets a
			// frozen column type equal the non-frozen declared type, so UdtCodec.accepts matches a
			// bound value.
			return getKeyspace().equals(that.getKeyspace()) && name.equals(that.getName())
				&& getFieldNames().equals(that.getFieldNames())
				&& getFieldTypes().equals(that.getFieldTypes());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(getKeyspace(), name, getFieldNames(), getFieldTypes());
	}

	public record UserDefinedTypeDefinition(@NonNull CqlIdentifier name,
											@NonNull DataType dataType) {

		public UserDefinedTypeDefinition {
			requireNonNull(name, "name must not be null");
			requireNonNull(dataType, "dataType must not be null");
		}

	}

}
