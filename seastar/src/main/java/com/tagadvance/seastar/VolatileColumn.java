package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.concurrent.locks.ReadWriteLock;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

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

	public VolatileColumn(SeaStarDriverContext context, SeaStarTable table,
		CqlIdentifier name, DataType type, boolean isStatic) {
		this.table = requireNonNull(table, "table must not be null");
		this.name = requireNonNull(name, "name must not be null");
		this.type = requireNonNull(type, "type must not be null");
		this.isStatic = isStatic;
		this.attachmentPoint = context;
	}

	/**
	 * The table's lock, which is its keyspace's; a column has none of its own. See the lock
	 * hierarchy in {@code AGENTS.md}.
	 */
	@Override
	public ReadWriteLock lock() {
		return table.lock();
	}

	@Override
	public SeaStarTable table() {
		return table;
	}

	@Override
	public CqlIdentifier getKeyspace() {
		return table.getKeyspace();
	}

	@Override
	public CqlIdentifier getParent() {
		return table.getName();
	}

	@Override
	public CqlIdentifier getTable() {
		return table.getName();
	}

	@Override
	public CqlIdentifier getName() {
		return name;
	}

	@Override
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
	public void attach(final AttachmentPoint attachmentPoint) {
		writeLock(() -> {
			this.attachmentPoint = requireNonNull(attachmentPoint,
				"attachmentPoint must not be null");
		});
	}

}
