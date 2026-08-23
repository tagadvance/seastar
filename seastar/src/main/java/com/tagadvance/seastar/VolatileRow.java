package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * One row of a table.
 *
 * <p>Holds no lock of its own. A row is only ever read or written alongside the column list its
 * values are positionally tied to, so the lock that guards the column list guards the row: the
 * keyspace's, reached through {@link #table}. That is also what deletes the deadlock a row lock
 * made possible, because there is no second lock left to take in the wrong order.
 */
@ThreadSafe
class VolatileRow implements SeaStarRow {

	private static final long serialVersionUID = 1L;

	/**
	 * Immutable.
	 */
	private final SeaStarDriverContext context;
	/**
	 * Immutable, and the owner of the lock every mutable field below is guarded by.
	 */
	private final SeaStarTable table;
	/**
	 * The table again, where it is the implementation this row is paired with, which is what knows
	 * about static columns and which columns are part of the key. Immutable.
	 */
	private final @Nullable VolatileTable volatileTable;
	/**
	 * The reference is immutable; the cells it holds are guarded by the table's lock.
	 */
	@GuardedBy("table.lock()")
	private final Cells cells;
	/**
	 * The cells this row's partition shares, or null until they are resolved. A static column is one
	 * cell per partition rather than one per row, so reads and writes of one are redirected here and
	 * every row of the partition sees the same value.
	 *
	 * <p>Resolved the first time a static column is touched rather than at construction, so that
	 * {@code ALTER TABLE ... ADD ... STATIC} reaches the rows that were already there. That makes the
	 * read path impure, and the read path holds only a read lock, so several threads may resolve at
	 * once. They all get the same {@link Cells} back - {@link VolatileTable#statics(List)} is a
	 * {@code computeIfAbsent} on a concurrent map - so the race is benign and a volatile write is
	 * enough to publish it. Null therefore means "not resolved yet" rather than "no statics": a row
	 * only asks when the column it is reading is declared static, and a static column means the
	 * table has one.
	 */
	private volatile @Nullable Cells statics;
	/**
	 * When the row's primary key stops being live, in seconds since the epoch, or
	 * {@link Cells#NEVER}. This is Cassandra's row marker: {@code INSERT ... USING TTL} expires the
	 * row itself, so a row whose every cell has gone still disappears at the right moment, while an
	 * ordinary INSERT leaves a row that outlives its columns.
	 */
	@GuardedBy("table.lock()")
	private long markerExpiresAt = Cells.NEVER;
	/**
	 * When the marker was written, in microseconds since the epoch, so that a delete stamped older
	 * than the insert it would remove is discarded like any other write.
	 */
	@GuardedBy("table.lock()")
	private long markerWriteTime;
	@GuardedBy("table.lock()")
	private AttachmentPoint attachmentPoint;

	protected VolatileRow(final @NonNull SeaStarDriverContext context,
		final @NonNull SeaStarTable table, final @NonNull List<Object> data) {
		this(context, table, data, Cells.microseconds(context.getClock()));
	}

	/**
	 * @param writeTime the microsecond timestamp every cell of the new row carries, which is what
	 *                  {@code writetime()} reports and what a later write is resolved against
	 */
	protected VolatileRow(final @NonNull SeaStarDriverContext context,
		final @NonNull SeaStarTable table, final @NonNull List<Object> data, final long writeTime) {
		this.context = requireNonNull(context, "context must not be null");
		this.table = requireNonNull(table, "table must not be null");
		this.volatileTable = table instanceof VolatileTable paired ? paired : null;
		// A copy, and a mutable one: ALTER TABLE ADD and DROP open and close a slot in every row, and
		// the caller's list may be immutable (SeaStarTable#addRow(Object...) hands over a List.of).
		this.cells = new Cells(validate(data), writeTime);
		this.markerWriteTime = writeTime;
		this.attachmentPoint = context;
		// A value written into a static slot belongs to the partition, not to this row. Nulls are
		// left alone: a row that simply does not name a static column must not clear it.
		for (int i = 0; i < data.size(); i++) {
			if (data.get(i) != null && volatileTable != null && volatileTable.isStatic(i)) {
				cellsOf(i).set(i, data.get(i), writeTime, Cells.NEVER);
			}
		}
	}

	private List<Object> validate(final @NonNull List<Object> values)
		throws IllegalArgumentException {
		requireNonNull(values, "values must not be null");

		table().readLock(() -> {
			if (values.size() != table().size()) {
				throw new IllegalArgumentException(
					"Expected %d values but got %d".formatted(table().size(), values.size()));
			}
			for (int i = 0; i < values.size(); i++) {
				validate(i, values.get(i));
			}
		});

		return values;
	}

	@Override
	public SeaStarDriverContext context() {
		return context;
	}

	@Override
	public SeaStarTable table() {
		return table;
	}

	/**
	 * The values this row stores, for {@link VolatileTable} to key it by its partition without
	 * round-tripping every value through its codec. Callers hold the table's lock.
	 */
	@GuardedBy("table.lock()")
	List<Object> storedValues() {
		return cells.values();
	}

	/**
	 * The cells a column at {@code i} lives in: the partition's, for a static column, and this row's
	 * for every other.
	 */
	private Cells cellsOf(final int i) {
		if (volatileTable == null || !volatileTable.isStatic(i)) {
			return cells;
		}
		var resolved = statics;
		if (resolved == null) {
			resolved = volatileTable.statics(cells.values());
			statics = resolved;
		}

		return resolved == null ? cells : resolved;
	}

	@Override
	public void set(final int i, final Object value) {
		set(i, value, Cells.microseconds(context.getClock()), Cells.NEVER);
	}

	@Override
	public boolean set(final int i, final @Nullable Object value, final long writeTime,
		final long expiresAt) {
		// The table's lock is the keyspace's, so this also excludes a concurrent schema change.
		return table.writeLockUnchecked(() -> cellsOf(i).set(i, value, writeTime, expiresAt));
	}

	@Override
	public void markLive(final long writeTime, final long expiresAt) {
		table.writeLock(() -> {
			if (writeTime < markerWriteTime) {
				return;
			}
			this.markerWriteTime = writeTime;
			this.markerExpiresAt = expiresAt;
		});
	}

	@Override
	public void clearMarker(final long timestamp) {
		table.writeLock(() -> {
			if (timestamp >= markerWriteTime) {
				this.markerExpiresAt = Long.MIN_VALUE;
			}
		});
	}

	@Override
	public boolean isLive() {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> {
			if (now < markerExpiresAt) {
				return true;
			}
			for (int i = 0; i < table.size(); i++) {
				if (!isKeyColumn(i) && cellsOf(i).isLive(i, now)) {
					return true;
				}
			}

			return false;
		});
	}

	private boolean isKeyColumn(final int i) {
		return volatileTable != null && volatileTable.isKeyColumn(i);
	}

	@Override
	public @Nullable Long writeTime(final int i) {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> cellsOf(i).writeTime(i, now));
	}

	@Override
	public @Nullable Integer ttl(final int i) {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> cellsOf(i).ttl(i, now));
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>Takes no lock: the table already holds its write lock across the whole column change, and
	 * that is the lock guarding this row.
	 */
	@Override
	public void insertValue(final int i, final @Nullable Object value) {
		cells.insert(i, value, Cells.microseconds(context.getClock()));
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>Takes no lock, for the same reason as {@link #insertValue(int, Object)}.
	 */
	@Override
	public void removeValue(final int i) {
		cells.remove(i);
	}

	@Override
	public Row snapshot() {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> {
			final var isDetached = isDetached();
			final var size = size();
			final var columnDefinitions = table.snapshot();
			final List<Object> d = new ArrayList<>(size);
			for (int i = 0; i < size; i++) {
				d.add(cellsOf(i).value(i, now));
			}

			return new Row() {

				@Override
				public boolean isDetached() {
					return isDetached;
				}

				/**
				 * A snapshot is a frozen copy with no live storage behind it to reattach, so this
				 * is refused rather than silently ignored.
				 */
				@Override
				public void attach(final @NonNull AttachmentPoint attachmentPoint) {
					throw new UnsupportedOperationException();
				}

				@Override
				@NonNull
				public CodecRegistry codecRegistry() {
					return VolatileRow.this.codecRegistry();
				}

				@Override
				@NonNull
				public ProtocolVersion protocolVersion() {
					return VolatileRow.this.protocolVersion();
				}

				@Override
				public int size() {
					return size;
				}

				@Override
				@NonNull
				public DataType getType(final int i) {
					return columnDefinitions.get(i).getType();
				}

				@Override
				public ByteBuffer getBytesUnsafe(final int i) {
					return codecRegistry().codecFor(getType(i)).encode(d.get(i), protocolVersion());
				}

				@Override
				public int firstIndexOf(final @NonNull String name) {
					return columnDefinitions.firstIndexOf(name);
				}

				@Override
				@NonNull
				public DataType getType(final @NonNull String name) {
					return columnDefinitions.get(name).getType();
				}

				@Override
				public int firstIndexOf(final @NonNull CqlIdentifier id) {
					return columnDefinitions.firstIndexOf(id);
				}

				@Override
				@NonNull
				public DataType getType(final @NonNull CqlIdentifier id) {
					return columnDefinitions.get(id).getType();
				}

				@Override
				@NonNull
				public ColumnDefinitions getColumnDefinitions() {
					return columnDefinitions;
				}

			};
		});
	}

	/**
	 * The table itself - a live view, not a copy: a column added or dropped by a schema change
	 * shows up here immediately. {@link #snapshot()} is the copy.
	 */
	@NonNull
	@Override
	public ColumnDefinitions getColumnDefinitions() {
		return table;
	}

	@Override
	public int size() {
		return table.size();
	}

	@NonNull
	@Override
	public DataType getType(int i) {
		return table.get(i).getType();
	}

	@NonNull
	@Override
	public List<Integer> allIndicesOf(@NonNull CqlIdentifier id) {
		final var indices = table.allIndicesOf(id);
		if (indices.isEmpty()) {
			// A caller almost always uses the result immediately (row.getString(name), etc.); an
			// empty list or -1 here would surface as a confusing out-of-bounds error deep in that
			// call instead of naming the missing column up front, so DefaultRow validates eagerly
			// and this mirrors it.
			throw new IllegalArgumentException("%s is not a column in this row".formatted(id));
		}

		return indices;
	}

	@Override
	public int firstIndexOf(@NonNull CqlIdentifier id) {
		final int indexOf = table.firstIndexOf(id);
		if (indexOf == -1) {
			// See the allIndicesOf(CqlIdentifier) overload just above for why this validates
			// eagerly rather than handing back -1.
			throw new IllegalArgumentException("%s is not a column in this row".formatted(id));
		}

		return indexOf;
	}

	@NonNull
	@Override
	public DataType getType(@NonNull CqlIdentifier id) {
		return table.readLockUnchecked(() -> {
			final var index = firstIndexOf(id);

			return table.get(index).getType();
		});
	}

	@NonNull
	@Override
	public List<Integer> allIndicesOf(@NonNull String name) {
		final var indices = table.allIndicesOf(name);
		if (indices.isEmpty()) {
			// A caller almost always uses the result immediately (row.getString(name), etc.); an
			// empty list or -1 here would surface as a confusing out-of-bounds error deep in that
			// call instead of naming the missing column up front, so DefaultRow validates eagerly
			// and this mirrors it.
			throw new IllegalArgumentException("%s is not a column in this row".formatted(name));
		}

		return indices;
	}

	@Override
	public int firstIndexOf(@NonNull String name) {
		final int indexOf = table.firstIndexOf(name);
		if (indexOf == -1) {
			// A caller almost always uses the result immediately (row.getString(name), etc.); an
			// empty list or -1 here would surface as a confusing out-of-bounds error deep in that
			// call instead of naming the missing column up front, so DefaultRow validates eagerly
			// and this mirrors it.
			throw new IllegalArgumentException("%s is not a column in this row".formatted(name));
		}

		return indexOf;
	}

	@NonNull
	@Override
	public DataType getType(@NonNull String name) {
		return table.readLockUnchecked(() -> {
			final var index = firstIndexOf(name);

			return table.get(index).getType();
		});
	}

	@NonNull
	@Override
	public CodecRegistry codecRegistry() {
		return table.readLockUnchecked(() -> attachmentPoint).getCodecRegistry();
	}

	@NonNull
	@Override
	public ProtocolVersion protocolVersion() {
		return table.readLockUnchecked(() -> attachmentPoint).getProtocolVersion();
	}

	@Override
	public boolean isDetached() {
		return table.readLockUnchecked(() -> attachmentPoint == AttachmentPoint.NONE);
	}

	/**
	 * Reattaches the row and its table together: the row answers {@link #getColumnDefinitions()}
	 * with the table, so the two must agree on where they are attached.
	 */
	@Override
	public void attach(final @NonNull AttachmentPoint attachmentPoint) {
		table.writeLock(() -> {
			this.attachmentPoint = requireNonNull(attachmentPoint,
				"attachmentPoint must not be null");
			this.table.attach(attachmentPoint);
		});
	}

	/**
	 * Encodes the stored value on demand, under the table's read lock: SeaStar stores Java objects
	 * rather than serialized bytes, so this is a serialization, not a lookup.
	 */
	@Nullable
	@Override
	public ByteBuffer getBytesUnsafe(int i) {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> codecRegistry().codecFor(getType(i))
			.encode(cellsOf(i).value(i, now), protocolVersion()));
	}

}
