package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A table and its rows.
 *
 * <p>Holds no lock of its own: {@link #lock()} hands back the keyspace's, so the column list, the
 * key definition and every row are guarded by one lock rather than by a level each. Every field
 * below is annotated with that guard or documented as immutable.
 *
 * <p>Rows are stored by partition. That is how a live node stores them, and it is what turns a point
 * lookup and a partition-wide delete from a scan of the table into a walk of one partition.
 */
@ThreadSafe
public class VolatileTable implements SeaStarTable {

	/**
	 * Immutable.
	 */
	private final UUID uuid = UUID.randomUUID();

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
	@GuardedBy("keyspace.lock()")
	private final List<SeaStarColumn> columns;
	@GuardedBy("keyspace.lock()")
	private final List<CqlIdentifier> partitionKey;
	@GuardedBy("keyspace.lock()")
	private final Map<CqlIdentifier, ClusteringOrder> clusteringColumns;
	@GuardedBy("keyspace.lock()")
	private final Map<CqlIdentifier, IndexMetadata> indexes;
	/**
	 * Every row, grouped by its partition key values and in insertion order within each partition -
	 * the partitions themselves in the order they were first written.
	 */
	@GuardedBy("keyspace.lock()")
	private final Map<List<Object>, List<SeaStarRow>> rowsByPartition;
	/**
	 * The static cells of each partition, keyed by that partition's key values. A static column
	 * belongs to the partition rather than to any row in it, so it is stored once here and every row
	 * of the partition reads and writes the same cell.
	 *
	 * <p>Guarded by the keyspace lock like everything else, except that {@link #statics(List)} adds
	 * to it while holding only the <em>read</em> lock - see that method. The map is concurrent so
	 * that two readers doing so at once is safe.
	 */
	@GuardedBy("keyspace.lock() for removal and iteration; concurrent for insertion")
	private final Map<List<Object>, Cells> staticsByPartition;
	@GuardedBy("keyspace.lock()")
	private AttachmentPoint attachmentPoint;

	public VolatileTable(final SeaStarDriverContext context, final SeaStarKeyspace keyspace,
		final CqlIdentifier name) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace = requireNonNull(keyspace, "keyspace must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.columns = new ArrayList<>();
		this.partitionKey = new ArrayList<>();
		this.clusteringColumns = new LinkedHashMap<>();
		this.indexes = new LinkedHashMap<>();
		this.rowsByPartition = new LinkedHashMap<>();
		this.staticsByPartition = new ConcurrentHashMap<>();
		this.attachmentPoint = context;
	}

	/**
	 * The static cells shared by the partition the given row values belong to, created empty the
	 * first time that partition is written, or null when the table declares no static column and so
	 * has nothing to share.
	 *
	 * <p>Takes no lock of its own, and adds to {@link #staticsByPartition} while its caller may hold
	 * only the read lock. A row resolves its partition while reading, and a read lock cannot be
	 * upgraded - taking the write lock here would deadlock, and did. The map is a
	 * {@link ConcurrentHashMap} instead, so {@code computeIfAbsent} is what makes creation atomic;
	 * every caller already holds at least the read lock over the column list this reads.
	 */
	@Nullable
	Cells statics(final List<Object> values) {
		if (columns.stream().noneMatch(SeaStarColumn::isStatic)) {
			return null;
		}

		return staticsByPartition.computeIfAbsent(partitionKeyOf(values),
			ignored -> new Cells(Collections.nCopies(columns.size(), null), 0L));
	}

	/**
	 * The partition a row of these values belongs to: its partition key column values, in key order.
	 * The key of both {@link #rowsByPartition} and {@link #staticsByPartition}.
	 *
	 * <p>An unmodifiable {@link ArrayList} rather than a {@code List.of}, because a partition key
	 * column that has not been written yet is null and {@code List.of} refuses one.
	 */
	@GuardedBy("keyspace.lock()")
	private List<Object> partitionKeyOf(final List<Object> values) {
		final List<Object> key = new ArrayList<>(partitionKey.size());
		for (final var column : partitionKey) {
			final var index = indexOf(column);
			key.add(index < 0 || index >= values.size() ? null : values.get(index));
		}

		return Collections.unmodifiableList(key);
	}

	/**
	 * The partition a row belongs to, read from what it stores rather than through
	 * {@link SeaStarRow#getObject(int)}, which round-trips every value through its codec.
	 */
	@GuardedBy("keyspace.lock()")
	private List<Object> partitionKeyOf(final SeaStarRow row) {
		if (row instanceof VolatileRow volatileRow) {
			return partitionKeyOf(volatileRow.storedValues());
		}
		final List<Object> key = new ArrayList<>(partitionKey.size());
		for (final var column : partitionKey) {
			final var index = indexOf(column);
			key.add(index < 0 ? null : row.getObject(index));
		}

		return Collections.unmodifiableList(key);
	}

	/**
	 * Rebuilds the index from the rows it already holds, for the changes that move a partition key
	 * column rather than a row. Every caller is already walking every row, so this costs no order.
	 */
	@GuardedBy("keyspace.lock()")
	private void reindex() {
		final var existing = rowsByPartition.values().stream().flatMap(List::stream).toList();
		rowsByPartition.clear();
		existing.forEach(row -> rowsByPartition.computeIfAbsent(partitionKeyOf(row),
			ignored -> new ArrayList<>()).add(row));
	}

	/**
	 * Whether the column at {@code i} is declared static, read without taking the lock for the
	 * callers that already hold it.
	 */
	boolean isStatic(final int i) {
		return columns.get(i).isStatic();
	}

	/**
	 * Whether the column at {@code i} is part of the primary key. A key column is not a cell that can
	 * expire, so it is what a row's liveness is judged apart from.
	 */
	boolean isKeyColumn(final int i) {
		final var name = columns.get(i).getName();

		return partitionKey.contains(name) || clusteringColumns.containsKey(name);
	}

	/**
	 * The keyspace's lock. A table has none of its own: a row is only ever mutated alongside the
	 * column list it is positionally tied to, so one lock over both is what keeps them consistent,
	 * and it leaves no pair of locks to take in the wrong order.
	 */
	@Override
	public ReadWriteLock lock() {
		return keyspace.lock();
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

		writeLock(() -> columns.add(column));
	}

	@Override
	public void markPartitionKey(final CqlIdentifier name) {
		requireNonNull(name, "name must not be null");

		writeLock(() -> {
			partitionKey.add(name);
			// The key definition changed, so every row belongs somewhere else now. A table normally
			// has no rows when this is called - CREATE TABLE builds the key first - but a test that
			// populates the model by hand may have.
			reindex();
		});
	}

	@Override
	public void markClustering(final CqlIdentifier name, final ClusteringOrder order) {
		requireNonNull(name, "name must not be null");
		requireNonNull(order, "order must not be null");

		writeLock(() -> clusteringColumns.put(name, order));
	}

	@Override
	public SeaStarColumn insertColumn(final CqlIdentifier name, final DataType type,
		final boolean isStatic) {
		requireNonNull(name, "name must not be null");
		requireNonNull(type, "type must not be null");

		final var column = new VolatileColumn(context, this, name, type, isStatic);

		return writeLockUnchecked(() -> {
			final var index = insertionIndexOf(name);
			columns.add(index, column);
			allRows().forEach(row -> row.insertValue(index, null));
			staticsByPartition.values().forEach(cells -> cells.insert(index, null, 0L));
			// A new column lands after the key columns, so no partition key value moves - but the
			// index is keyed by position, so it is rebuilt rather than reasoned about.
			reindex();

			return column;
		});
	}

	@Override
	public void removeColumn(final CqlIdentifier name) {
		requireNonNull(name, "name must not be null");

		writeLock(() -> {
			final var index = indexOf(name);
			if (index < 0) {
				return;
			}

			columns.remove(index);
			allRows().forEach(row -> row.removeValue(index));
			staticsByPartition.values().forEach(cells -> cells.remove(index));
			reindex();
		});
	}

	@Override
	public void renameColumn(final CqlIdentifier from, final CqlIdentifier to) {
		requireNonNull(from, "from must not be null");
		requireNonNull(to, "to must not be null");

		writeLock(() -> {
			final var index = indexOf(from);
			if (index < 0) {
				return;
			}

			final var current = columns.get(index);
			columns.set(index, new VolatileColumn(context, this, to, current.getType(),
				current.isStatic()));
			Collections.replaceAll(partitionKey, from, to);
			if (clusteringColumns.containsKey(from)) {
				// Rebuilt rather than removed and re-put: the map is ordered, and a re-put would move
				// the renamed column to the end of the clustering key.
				final Map<CqlIdentifier, ClusteringOrder> renamed = new LinkedHashMap<>();
				clusteringColumns.forEach((id, order) -> renamed.put(from.equals(id) ? to : id, order));
				clusteringColumns.clear();
				clusteringColumns.putAll(renamed);
			}
		});
	}

	/**
	 * Where a newly added column goes: after the primary key columns, which the column list holds
	 * first, and alphabetically among the ones that follow. That is the order a live node reports,
	 * and it is the order {@code CreateTableHandler} builds the list in.
	 */
	private int insertionIndexOf(final CqlIdentifier name) {
		final var keyColumns = partitionKey.size() + clusteringColumns.size();

		return IntStream.range(keyColumns, columns.size())
			.filter(i -> columns.get(i).getName().asInternal().compareTo(name.asInternal()) > 0)
			.findFirst()
			.orElse(columns.size());
	}

	/**
	 * The index of a column, read without taking the lock, for the callers that already hold it.
	 */
	private int indexOf(final CqlIdentifier id) {
		return IntStream.range(0, columns.size())
			.filter(index -> columns.get(index).getName().equals(id))
			.findFirst()
			.orElse(-1);
	}

	private SeaStarColumn columnByName(final CqlIdentifier id) {
		return columns.stream().filter(column -> column.getName().equals(id)).findFirst().orElse(null);
	}

	@Override
	public void addRow(final SeaStarRow row) {
		requireNonNull(row, "row must not be null");

		writeLock(() -> rowsByPartition.computeIfAbsent(partitionKeyOf(row),
			ignored -> new ArrayList<>()).add(row));
	}

	@Override
	public void removeRowIf(final Predicate<SeaStarRow> predicate) {
		writeLock(() -> {
			rowsByPartition.values().forEach(partition -> partition.removeIf(predicate));
			rowsByPartition.values().removeIf(List::isEmpty);
		});
	}

	@Override
	public void removeRowIf(final List<Object> partitionKeyValues,
		final Predicate<SeaStarRow> predicate) {
		requireNonNull(partitionKeyValues, "partitionKeyValues must not be null");

		writeLock(() -> {
			final var partition = rowsByPartition.get(
				Collections.unmodifiableList(new ArrayList<>(partitionKeyValues)));
			if (partition == null) {
				return;
			}
			partition.removeIf(predicate);
			if (partition.isEmpty()) {
				rowsByPartition.values().removeIf(List::isEmpty);
			}
		});
	}

	/**
	 * A snapshot of every row, taken under the read lock. The stream is consumed wherever the caller
	 * pleases, so handing out a lazy view over the live storage would be handing out a
	 * {@link java.util.ConcurrentModificationException}.
	 */
	@Override
	public Stream<SeaStarRow> rows() {
		return readLockUnchecked(() -> allRows().toList()).stream();
	}

	@Override
	public Stream<SeaStarRow> partition(final List<Object> partitionKeyValues) {
		requireNonNull(partitionKeyValues, "partitionKeyValues must not be null");

		return readLockUnchecked(() -> List.copyOf(rowsByPartition.getOrDefault(
			Collections.unmodifiableList(new ArrayList<>(partitionKeyValues)), List.of()))).stream();
	}

	/**
	 * Every row, lazily, for the callers that already hold the lock.
	 */
	@GuardedBy("keyspace.lock()")
	private Stream<SeaStarRow> allRows() {
		return rowsByPartition.values().stream().flatMap(List::stream);
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
	public void addIndex(final IndexMetadata index) {
		requireNonNull(index, "index must not be null");

		writeLock(() -> indexes.put(index.getName(), index));
	}

	@Override
	public void removeIndex(final CqlIdentifier name) {
		requireNonNull(name, "name must not be null");

		writeLock(() -> indexes.remove(name));
	}

	@Override
	@NonNull
	public Map<CqlIdentifier, IndexMetadata> getIndexes() {
		return readLockUnchecked(() -> Map.copyOf(indexes));
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
		return readLockUnchecked(() -> partitionKey.stream()
			.map(this::columnByName)
			.filter(Objects::nonNull)
			.map(ColumnMetadata.class::cast)
			.toList());
	}

	@Override
	@NonNull
	public Map<ColumnMetadata, ClusteringOrder> getClusteringColumns() {
		return readLockUnchecked(() -> {
			final Map<ColumnMetadata, ClusteringOrder> result = new LinkedHashMap<>();
			clusteringColumns.forEach((id, order) -> {
				final var column = columnByName(id);
				if (column != null) {
					result.put(column, order);
				}
			});

			return Collections.unmodifiableMap(result);
		});
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
		writeLock(() -> {
			rowsByPartition.clear();
			staticsByPartition.clear();
		});
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
