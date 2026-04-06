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
	private final List<Object> data;
	private AttachmentPoint attachmentPoint;

	protected VolatileRow(final @NonNull SeaStarDriverContext context,
		final @NonNull SeaStarTable table, final @NonNull List<Object> data) {
		this.context = requireNonNull(context, "context must not be null");
		this.table = requireNonNull(table, "table must not be null");
		this.data = validate(data);
		this.attachmentPoint = context;
	}

	private List<Object> validate(final @NonNull List<Object> values)
		throws IllegalArgumentException {
		requireNonNull(values, "values must not be null");

		table().readLock(() -> {
			checkArgument(values.size() == table().size(), "Expected %d values but got %d",
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

	@Override
	public void set(final int i, final Object value) {
		// lock the table to precent concurrent schema changes
		table.readLock(() -> writeLock(() -> {
			data.set(i, value);
		}));
	}

	@Override
	public Row snapshot() {
		return table.readLockUnchecked(() -> readLockUnchecked(() -> {
			final var isDetached = isDetached();
			final var size = size();
			final var columnDefinitions = table.snapshot();
			final var d = List.copyOf(data);

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
		return readLockUnchecked(
			() -> codecRegistry().codecFor(getType(i)).encode(data.get(i), protocolVersion()));
	}

}
