package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class VolatileTable implements SeaStarTable {

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final UUID uuid = UUID.randomUUID();

	private final SeaStarDriverContext context;
	private final SeaStarKeyspace keyspace;
	private final CqlIdentifier name;
	private final List<SeaStarColumn> columns;
	private final List<SeaStarRow> rows;
	private AttachmentPoint attachmentPoint;

	public VolatileTable(final SeaStarDriverContext context, final SeaStarKeyspace keyspace,
		final CqlIdentifier name) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace = requireNonNull(keyspace, "keyspace must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.columns = new ArrayList<>();
		this.rows = new ArrayList<>();
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
	public SeaStarKeyspace keyspace() {
		return keyspace;
	}

	@Override
	public void addColumn(final SeaStarColumn column) {
		requireNonNull(column, "column must not be null");

		writeLock(() -> {
			columns.add(column);
		});
	}

	@Override
	public void addRow(final SeaStarRow row) {
		requireNonNull(row, "row must not be null");

		writeLock(() -> {
			rows.add(row);
		});
	}

	@Override
	public void removeRowIf(final Predicate<SeaStarRow> predicate) {
		writeLock(() -> {
			rows.removeIf(predicate);
		});
	}

	@Override
	public Stream<SeaStarRow> rows() {
		return rows.stream();
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
		return keyspace.name();
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
		return readLockUnchecked(() -> columns.stream()
			.collect(Collectors.toUnmodifiableMap(ColumnMetadata::getName, Function.identity())));
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, Object> getOptions() {
		return Collections.emptyMap();
	}

	@Override
	public int size() {
		return readLockUnchecked(columns::size);
	}

	@Override
	@NonNull
	public ColumnDefinition get(final int i) {
		return readLockUnchecked(() -> columns.get(i));
	}

	@Override
	public boolean contains(final @NonNull String name) {
		return contains(CqlIdentifier.fromInternal(name));
	}

	@Override
	public boolean contains(final @NonNull CqlIdentifier id) {
		return readLockUnchecked(
			() -> columns.stream().map(ColumnMetadata::getName).anyMatch(id::equals));
	}

	@Override
	public int firstIndexOf(final @NonNull String name) {
		return firstIndexOf(CqlIdentifier.fromInternal(name));
	}

	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		return readLockUnchecked(() -> IntStream.range(0, columns.size())
			.filter(index -> columns.get(index).getName().equals(id))
			.findFirst()
			.orElse(-1));
	}

	@Override
	public void drop() {
		attach(AttachmentPoint.NONE);
	}

	@Override
	public void truncate() {
		writeLock(rows::clear);
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
			columns.forEach(column -> column.attach(attachmentPoint));
		});
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
		return readLockUnchecked(
			() -> columns.stream().map(ColumnDefinition.class::cast).toList().iterator());
	}

	@Override
	public ColumnDefinitions snapshot() {
		return readLockUnchecked(() -> {
			final var columnDefinitions = StreamSupport.stream(spliterator(), false).toList();
			final boolean isDetached = isDetached();
			final int size = size();

			return new ColumnDefinitions() {

				@Override
				@NonNull
				public Iterator<ColumnDefinition> iterator() {
					return columnDefinitions.iterator();
				}

				@Override
				public boolean isDetached() {
					return isDetached;
				}

				@Override
				public void attach(final @NonNull AttachmentPoint attachmentPoint) {
					throw new UnsupportedOperationException();
				}

				@Override
				public int size() {
					return size;
				}

				@Override
				@NonNull
				public ColumnDefinition get(final int i) {
					return columnDefinitions.get(i);
				}

				@Override
				public boolean contains(final @NonNull String name) {
					requireNonNull(name, "name must not be null");

					return columnDefinitions.stream()
						.map(ColumnDefinition::getName)
						.map(CqlIdentifier::asInternal)
						.anyMatch(name::equals);
				}

				@Override
				public boolean contains(final @NonNull CqlIdentifier id) {
					requireNonNull(id, "id must not be null");

					return columnDefinitions.stream()
						.map(ColumnDefinition::getName)
						.anyMatch(id::equals);
				}

				@Override
				public int firstIndexOf(final @NonNull String name) {
					requireNonNull(name, "name must not be null");

					return firstIndexOf(CqlIdentifier.fromInternal(name));
				}

				@Override
				public int firstIndexOf(final @NonNull CqlIdentifier id) {
					requireNonNull(id, "id must not be null");

					return IntStream.range(0, columnDefinitions.size())
						.filter(i -> columnDefinitions.get(i).getName().equals(id))
						.findFirst()
						.orElse(-1);
				}

			};
		});
	}

}
