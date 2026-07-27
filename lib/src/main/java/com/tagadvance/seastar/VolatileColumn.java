package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.concurrent.locks.ReadWriteLock;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * One column of a table. Holds no lock of its own; it is guarded by its table's, which is its
 * keyspace's. See the lock hierarchy in {@code AGENTS.md}.
 */
@ThreadSafe
class VolatileColumn implements SeaStarColumn {

	/**
	 * Immutable, and the owner of the lock the mutable field below is guarded by.
	 */
	private final SeaStarTable table;
	/**
	 * Immutable.
	 */
	private final CqlIdentifier name;
	/**
	 * Immutable.
	 */
	private final DataType type;
	/**
	 * Immutable.
	 */
	private final boolean isStatic;
	@GuardedBy("table.lock()")
	private AttachmentPoint attachmentPoint;

	public VolatileColumn(@NonNull SeaStarDriverContext context, @NonNull SeaStarTable table,
		@NonNull CqlIdentifier name, @NonNull DataType type, boolean isStatic) {
		this.table = requireNonNull(table, "table must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.type = requireNonNull(type, "type must not be null");
		this.isStatic = isStatic;
		this.attachmentPoint = context;
	}

	@Override
	public ReadWriteLock lock() {
		return table.lock();
	}

	@Override
	public SeaStarTable table() {
		return table;
	}

	@Override
	@NonNull
	public CqlIdentifier getKeyspace() {
		return table.getKeyspace();
	}

	@Override
	@NonNull
	public CqlIdentifier getParent() {
		return table.getName();
	}

	@Override
	@NonNull
	public CqlIdentifier getTable() {
		return table.getName();
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
		return readLockUnchecked(() -> attachmentPoint == AttachmentPoint.NONE);
	}

	@Override
	public void attach(final @NonNull AttachmentPoint attachmentPoint) {
		writeLock(() -> {
			this.attachmentPoint = requireNonNull(attachmentPoint,
				"attachmentPoint must not be null");
		});
	}

}
