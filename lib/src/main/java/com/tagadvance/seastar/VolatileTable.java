package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.tagadvance.tools.Locks;
import com.tagadvance.tools.Locks.SeaStarReadWriteLock;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class VolatileTable implements SeaStarTable {

	private final SeaStarReadWriteLock lock = Locks.newLock();

	private final UUID uuid = UUID.randomUUID();

	private final SeaStarDriverContext context;
	private final CqlIdentifier keyspace;
	private final CqlIdentifier name;
	private final List<VolatileColumn> columns;
	private final AtomicBoolean isDetached;

	public VolatileTable(final SeaStarDriverContext context, final CqlIdentifier keyspace,
		final CqlIdentifier name, final List<VolatileColumn> columns, final boolean isDetached) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace = requireNonNull(keyspace, "keyspace must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.columns = List.copyOf(requireNonNull(columns, "columns must not be null"));
		this.isDetached = new AtomicBoolean(isDetached);
	}

	@Override
	public SeaStarDriverContext context() {
		return context;
	}

	@Override
	public boolean isCompactStorage() {
		return false;
	}

	@Override
	public boolean isVirtual() {
		return false;
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, IndexMetadata> getIndexes() {
		// FIXME
		throw new UnsupportedOperationException("Indexes are not supported in SeaStarTable");
	}

	@Override
	@NonNull
	public CqlIdentifier getKeyspace() {
		return keyspace;
	}

	@Override
	@NonNull
	public CqlIdentifier getName() {
		return name;
	}

	@Override
	public Optional<UUID> getId() {
		return Optional.of(uuid);
	}

	@Override
	@NonNull
	public List<ColumnMetadata> getPartitionKey() {
		// FIXME don't forget to lock
		return List.of();
	}

	@Override
	@NonNull
	public Map<ColumnMetadata, ClusteringOrder> getClusteringColumns() {
		// FIXME don't forget to lock
		return Map.of();
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, ColumnMetadata> getColumns() {
		return lock.readLockUnchecked(() -> columns.stream()
			.collect(Collectors.toUnmodifiableMap(ColumnMetadata::getName, Function.identity())));
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, Object> getOptions() {
		return Collections.emptyMap();
	}

	@Override
	public int size() {
		return lock.readLockUnchecked(columns::size);
	}

	@Override
	@NonNull
	public ColumnDefinition get(final int i) {
		return lock.readLockUnchecked(() -> columns.get(i));
	}

	@Override
	public boolean contains(final @NonNull String name) {
		return contains(CqlIdentifier.fromInternal(name));
	}

	@Override
	public boolean contains(final @NonNull CqlIdentifier id) {
		return lock.readLockUnchecked(
			() -> columns.stream().map(ColumnMetadata::getName).anyMatch(id::equals));
	}

	@Override
	public int firstIndexOf(final @NonNull String name) {
		return firstIndexOf(CqlIdentifier.fromInternal(name));
	}

	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		return lock.readLockUnchecked(() -> IntStream.range(0, columns.size())
			.filter(index -> columns.get(index).getName().equals(id))
			.findFirst()
			.orElse(-1));
	}

	@Override
	public void detach() {
		this.isDetached.set(true);
	}

	@Override
	public boolean isDetached() {
		return isDetached.get();
	}

	@Override
	public void attach(final @NonNull AttachmentPoint attachmentPoint) {
		throw new UnsupportedOperationException();
	}


	/**
	 * Create an {@link Iterator iterator} of a copy of the {@link ColumnDefinition} to avoid
	 * concurrent modification.
	 *
	 * @return an {@link Iterator iterator}
	 */
	@Override
	@NonNull
	public Iterator<ColumnDefinition> iterator() {
		return lock.readLockUnchecked(
			() -> columns.stream().map(ColumnDefinition.class::cast).toList().iterator());
	}

	public record VolatileColumn(@NonNull CqlIdentifier keyspace, @NonNull CqlIdentifier table,
								 @NonNull CqlIdentifier name, @NonNull DataType type,
								 boolean isStatic, boolean isDetached) implements ColumnDefinition,
		ColumnMetadata {

		public VolatileColumn {
			requireNonNull(keyspace, "keyspace must not be null");
			requireNonNull(table, "table must not be null");
			requireNonNull(name, "name must not be null");
			requireNonNull(type, "type must not be null");
		}

		@Override
		@NonNull
		public CqlIdentifier getKeyspace() {
			return keyspace;
		}

		@Override
		@NonNull
		public CqlIdentifier getParent() {
			return table;
		}

		@Override
		@NonNull
		public CqlIdentifier getTable() {
			return table;
		}

		@Override
		@NonNull
		public CqlIdentifier getName() {
			return name;
		}

		@Override
		@NonNull
		public DataType getType() {
			return type;
		}

		@Override
		public boolean isStatic() {
			return isStatic;
		}

		@Override
		public boolean isDetached() {
			return isDetached;
		}

		@Override
		public void attach(final @NonNull AttachmentPoint attachmentPoint) {
			throw new UnsupportedOperationException();
		}

	}

}
