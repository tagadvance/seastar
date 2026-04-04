package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.tagadvance.tools.Locks;
import com.tagadvance.tools.Locks.SeaStarReadWriteLock;
import java.util.List;
import java.util.stream.IntStream;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class VolatileUserDefinedType implements SeaStarUserDefinedType {

	private final SeaStarReadWriteLock lock = Locks.newLock();

	private final SeaStarDriverContext context;
	private final CqlIdentifier keyspace;
	private final CqlIdentifier name;
	private final boolean isFrozen;
	private final List<UserDefinedTypeDefinition> definitions;

	public VolatileUserDefinedType(final SeaStarDriverContext context, final CqlIdentifier keyspace, final CqlIdentifier name,
		final boolean isFrozen, final List<UserDefinedTypeDefinition> definitions) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace = requireNonNull(keyspace, "keyspace must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.isFrozen = isFrozen;
		this.definitions = requireNonNull(definitions, "definitions must not be null");
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
		return lock.readLockUnchecked(() -> definitions.stream().map(UserDefinedTypeDefinition::name).toList());
	}

	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		return lock.readLockUnchecked(() -> IntStream.range(0, definitions.size())
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
		return lock.readLockUnchecked(() -> definitions.stream().map(UserDefinedTypeDefinition::dataType).toList());
	}

	@Override
	@NonNull
	public UserDefinedType copy(final boolean newFrozen) {
		return new VolatileUserDefinedType(context, keyspace, name, newFrozen, definitions);
	}

	@Override
	@NonNull
	public SeaStarUdtValue newValue() {
		return new VolatileUdtValue(this);
	}

	@Override
	@NonNull
	public UdtValue newValue(final Object @NonNull ... values) {
		return new VolatileUdtValue(this, values);
	}

	@Override
	@NonNull
	public AttachmentPoint getAttachmentPoint() {
		return context;
	}

	@Override
	public boolean isDetached() {
		// TODO: detached if not yet persisted
		return false;
	}

	@Override
	public void attach(final @NonNull AttachmentPoint attachmentPoint) {
		throw new UnsupportedOperationException();
	}

	public record UserDefinedTypeDefinition(@NonNull CqlIdentifier name, @NonNull DataType dataType) {

		public UserDefinedTypeDefinition {
			requireNonNull(name, "name must not be null");
			requireNonNull(dataType, "dataType must not be null");
		}

	}

}
