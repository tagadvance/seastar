package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import org.jspecify.annotations.Nullable;

/**
 * A row of a table.
 *
 * <p>A row carries no lock of its own; it is guarded by the lock of the table it belongs to, which
 * is the lock of that table's keyspace. See the lock hierarchy in {@code AGENTS.md}.
 */
public interface SeaStarRow extends Row {

	SeaStarDriverContext context();

	SeaStarTable table();

	/**
	 * Resolving the column and writing it are one locked region, so the column cannot be dropped in
	 * between and turn the index into someone else's column.
	 */
	default void set(final String name, final Object value) {
		table().writeLock(() -> set(table().firstIndexOf(name), value));
	}

	/**
	 * @see #set(String, Object)
	 */
	default void set(final CqlIdentifier id, final Object value) {
		table().writeLock(() -> set(table().firstIndexOf(id), value));
	}

	void set(int i, Object value);

	/**
	 * Writes a column with the write time and expiry a statement asked for, and answers whether the
	 * write took: Cassandra resolves two writes to one cell by their timestamps, so a write stamped
	 * older than what is already stored is discarded.
	 *
	 * @param expiresAt when the value stops being readable, in seconds since the epoch, or
	 *                  {@link Long#MAX_VALUE} for a column written without a TTL
	 */
	boolean set(int i, @Nullable Object value, long writeTime, long expiresAt);

	/**
	 * Sets when this row's primary key stops being live, in seconds since the epoch. That is
	 * Cassandra's row marker: {@code INSERT ... USING TTL} expires the row itself, so the row goes
	 * when its columns do rather than lingering as a key with nothing in it.
	 *
	 * @param writeTime the microsecond timestamp the marker is written at, which is what a later
	 *                  {@code DELETE ... USING TIMESTAMP} is resolved against
	 */
	void markLive(long writeTime, long expiresAt);

	/**
	 * Ends the row's marker if the given timestamp is at or after the one it was written at, which is
	 * how a {@code DELETE ... USING TIMESTAMP} older than the INSERT it would remove leaves the row
	 * standing.
	 */
	void clearMarker(long timestamp);

	/**
	 * Whether this row is still readable: its marker has not expired, or some column outside the
	 * primary key still holds an unexpired value.
	 */
	boolean isLive();

	/**
	 * The microsecond timestamp a column was written at, or null when it holds nothing - which is
	 * what a cluster answers for {@code writetime()} of a null column.
	 */
	@Nullable
	Long writeTime(int i);

	/**
	 * The seconds a column has left before it expires, or null when it holds nothing or was written
	 * without a TTL.
	 */
	@Nullable
	Integer ttl(int i);

	/**
	 * Opens a slot at {@code i} and fills it with {@code value}, shifting the values after it along.
	 * A row's values are positional, tied to its table's column list, so a column added to the table
	 * has to be added to every row at the same index.
	 *
	 * <p>Called from the table with its write lock already held, which is what keeps the row and the
	 * column list in step.
	 */
	void insertValue(int i, @Nullable Object value);

	/**
	 * Removes the slot at {@code i}, shifting the values after it back. The counterpart of
	 * {@link #insertValue(int, Object)}, for a column dropped from the table.
	 */
	void removeValue(int i);

	default void validate(final int i, final Object value) {
		final var dataType = getColumnDefinitions().get(i).getType();
		final var codec = context().getCodecRegistry().codecFor(dataType);
		if (value != null && !codec.accepts(value)) {
			throw new IllegalArgumentException(
				"Value at index %d (%s) is not compatible with column type %s".formatted(i, value,
					dataType));
		}
	}

	Row snapshot();

}
