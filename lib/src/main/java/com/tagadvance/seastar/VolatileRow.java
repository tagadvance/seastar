package com.tagadvance.seastar;

import static com.google.common.base.Preconditions.checkArgument;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

@ThreadSafe
public class VolatileRow implements SeaStarRow {

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final SeaStarDriverContext context;
	private final SeaStarTable table;
	/**
	 * The table again, where it is the implementation this row is paired with, which is what knows
	 * about static columns and which columns are part of the key.
	 */
	private final @Nullable VolatileTable volatileTable;
	private final Cells cells;
	/**
	 * The cells this row's partition shares, or null for a table with no static columns. A static
	 * column is one cell per partition rather than one per row, so reads and writes of one are
	 * redirected here and every row of the partition sees the same value.
	 *
	 * <p>Resolved the first time a static column is touched rather than at construction, so that
	 * {@code ALTER TABLE ... ADD ... STATIC} reaches the rows that were already there.
	 */
	private @Nullable Cells statics;
	private boolean staticsResolved;
	/**
	 * When the row's primary key stops being live, in seconds since the epoch, or
	 * {@link Cells#NEVER}. This is Cassandra's row marker: {@code INSERT ... USING TTL} expires the
	 * row itself, so a row whose every cell has gone still disappears at the right moment, while an
	 * ordinary INSERT leaves a row that outlives its columns.
	 */
	private long markerExpiresAt = Cells.NEVER;
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
			checkArgument(values.size() == table().size(), "Expected %s values but got %s",
				table().size(), values.size());
			for (int i = 0; i < values.size(); i++) {
				validate(i, values.get(i));
			}
		});

		return values;
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
	public SeaStarTable table() {
		return table;
	}

	/**
	 * The cells a column at {@code i} lives in: the partition's, for a static column, and this row's
	 * for every other.
	 */
	private Cells cellsOf(final int i) {
		if (volatileTable == null || !volatileTable.isStatic(i)) {
			return cells;
		}
		if (!staticsResolved) {
			statics = volatileTable.statics(cells.values());
			staticsResolved = true;
		}

		return statics == null ? cells : statics;
	}

	@Override
	public void set(final int i, final Object value) {
		set(i, value, Cells.microseconds(context.getClock()), Cells.NEVER);
	}

	@Override
	public boolean set(final int i, final @Nullable Object value, final long writeTime,
		final long expiresAt) {
		// lock the table to prevent concurrent schema changes
		return table.readLockUnchecked(
			() -> writeLockUnchecked(() -> cellsOf(i).set(i, value, writeTime, expiresAt)));
	}

	@Override
	public void markLive(final long expiresAt) {
		writeLock(() -> this.markerExpiresAt = expiresAt);
	}

	@Override
	public boolean isLive() {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> readLockUnchecked(() -> {
			if (now < markerExpiresAt) {
				return true;
			}
			for (int i = 0; i < table.size(); i++) {
				if (!isKeyColumn(i) && cellsOf(i).isLive(i, now)) {
					return true;
				}
			}

			return false;
		}));
	}

	private boolean isKeyColumn(final int i) {
		return volatileTable != null && volatileTable.isKeyColumn(i);
	}

	@Override
	public @Nullable Long writeTime(final int i) {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> readLockUnchecked(() -> cellsOf(i).writeTime(i, now)));
	}

	@Override
	public @Nullable Integer ttl(final int i) {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> readLockUnchecked(() -> cellsOf(i).ttl(i, now)));
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>Takes only this row's lock: the table holds its own write lock across the whole column
	 * change, so taking it again here would be redundant.
	 */
	@Override
	public void insertValue(final int i, final @Nullable Object value) {
		writeLock(() -> cells.insert(i, value, Cells.microseconds(context.getClock())));
	}

	@Override
	public void removeValue(final int i) {
		writeLock(() -> cells.remove(i));
	}

	@Override
	public Row snapshot() {
		final var now = Cells.seconds(context.getClock());

		return table.readLockUnchecked(() -> readLockUnchecked(() -> {
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
		}));
	}

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
			// copied from DefaultRow; not sure why this is necessary
			throw new IllegalArgumentException("%s is not a column in this row".formatted(id));
		}

		return indices;
	}

	@Override
	public int firstIndexOf(@NonNull CqlIdentifier id) {
		final int indexOf = table.firstIndexOf(id);
		if (indexOf == -1) {
			// copied from DefaultRow
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
			// copied from DefaultRow; not sure why this is necessary
			throw new IllegalArgumentException("%s is not a column in this row".formatted(name));
		}

		return indices;
	}

	@Override
	public int firstIndexOf(@NonNull String name) {
		final int indexOf = table.firstIndexOf(name);
		if (indexOf == -1) {
			// copied from DefaultRow; not sure why this is necessary
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
		return attachmentPoint.getCodecRegistry();
	}

	@NonNull
	@Override
	public ProtocolVersion protocolVersion() {
		return attachmentPoint.getProtocolVersion();
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
			this.table.attach(attachmentPoint);
		});
	}

	@Nullable
	@Override
	public ByteBuffer getBytesUnsafe(int i) {
		final var now = Cells.seconds(context.getClock());

		return readLockUnchecked(() -> codecRegistry().codecFor(getType(i))
			.encode(cellsOf(i).value(i, now), protocolVersion()));
	}

}
