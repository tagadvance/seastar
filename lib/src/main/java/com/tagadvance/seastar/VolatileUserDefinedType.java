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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class VolatileUserDefinedType implements SeaStarUserDefinedType {

	private final ReadWriteLock lock;

	private final SeaStarDriverContext context;
	private final CqlIdentifier keyspace;
	private final CqlIdentifier name;
	private final boolean isFrozen;
	private final List<UserDefinedTypeDefinition> definitions;
	private AttachmentPoint attachmentPoint;

	public VolatileUserDefinedType(final SeaStarDriverContext context, final CqlIdentifier keyspace,
		final CqlIdentifier name, final boolean isFrozen,
		final List<UserDefinedTypeDefinition> definitions) {
		this(context, keyspace, name, isFrozen,
			new ArrayList<>(requireNonNull(definitions, "definitions must not be null")),
			new ReentrantReadWriteLock());
	}

	private VolatileUserDefinedType(final SeaStarDriverContext context, final CqlIdentifier keyspace,
		final CqlIdentifier name, final boolean isFrozen,
		final List<UserDefinedTypeDefinition> definitions, final ReadWriteLock lock) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace = requireNonNull(keyspace, "keyspace must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.isFrozen = isFrozen;
		this.definitions = definitions;
		this.lock = lock;
		this.attachmentPoint = context;
	}

	@Override
	public ReadWriteLock lock() {
		return lock;
	}

	@Override
	public SeaStarDriverContext context() {
		return context;
	}

	@Override
	public CqlIdentifier getKeyspace() {
		return keyspace;
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
	public List<DataType> getFieldTypes() {
		return readLockUnchecked(
			() -> definitions.stream().map(UserDefinedTypeDefinition::dataType).toList());
	}

	// Copies share the definitions and the lock so that ALTER TYPE stays visible through every frozen
	// and non-frozen variant handed out as a column type, the way schema propagates on a cluster.
	@Override
	@NonNull
	public UserDefinedType copy(final boolean newFrozen) {
		return new VolatileUserDefinedType(context, keyspace, name, newFrozen, definitions, lock);
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
			return keyspace.equals(that.getKeyspace()) && name.equals(that.getName())
				&& getFieldNames().equals(that.getFieldNames())
				&& getFieldTypes().equals(that.getFieldTypes());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(keyspace, name, getFieldNames(), getFieldTypes());
	}

	public record UserDefinedTypeDefinition(@NonNull CqlIdentifier name,
											@NonNull DataType dataType) {

		public UserDefinedTypeDefinition {
			requireNonNull(name, "name must not be null");
			requireNonNull(dataType, "dataType must not be null");
		}

	}

}
