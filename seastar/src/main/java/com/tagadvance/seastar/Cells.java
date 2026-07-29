package com.tagadvance.seastar;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * The values of one row, or the static values shared by one partition, together with the write time
 * and expiry Cassandra keeps per cell.
 *
 * <p>Positional: slot {@code i} belongs to column {@code i} of the table, and a column added or
 * dropped opens or closes the same slot here as it does in every other row. A static cell is stored
 * once per partition rather than once per row, so a {@code Cells} that backs a partition's statics
 * is the same width as a row's and only its static slots are ever read.
 *
 * <p>Expiry is evaluated lazily, on read, against the session's clock: nothing is reaped and no
 * timer runs, so a test advances a {@link SeaStarClock} rather than sleeping.
 *
 * <p>Not thread-safe; the owner - a {@link VolatileRow} or a {@link VolatileTable} - holds the lock.
 */
@NotThreadSafe
final class Cells {

	/**
	 * The expiry of a cell that does not expire.
	 */
	static final long NEVER = Long.MAX_VALUE;

	private final List<Object> values;
	/**
	 * Microseconds since the epoch, which is the unit {@code writetime()} reports.
	 */
	private final List<Long> writeTimes;
	/**
	 * Seconds since the epoch, which is the unit a TTL counts in, or {@link #NEVER}.
	 */
	private final List<Long> expiries;

	Cells(final List<Object> values, final long writeTime) {
		this.values = new ArrayList<>(values);
		this.writeTimes = new ArrayList<>(Collections.nCopies(values.size(), writeTime));
		this.expiries = new ArrayList<>(Collections.nCopies(values.size(), NEVER));
	}

	/**
	 * The write time of a statement executed against the given clock, in microseconds since the
	 * epoch - the unit Cassandra stamps a cell with and {@code writetime()} reports.
	 */
	static long microseconds(final Clock clock) {
		final var instant = clock.instant();

		return Math.multiplyExact(instant.getEpochSecond(), 1_000_000L) + instant.getNano() / 1_000;
	}

	/**
	 * The current time in seconds since the epoch, which is the resolution a TTL expires at.
	 */
	static long seconds(final Clock clock) {
		return clock.instant().getEpochSecond();
	}

	int size() {
		return values.size();
	}

	/**
	 * The stored values, ignoring expiry, for the callers that key off them rather than read them -
	 * finding a row's partition among the partitions of its table.
	 */
	List<Object> values() {
		return values;
	}

	void insert(final int i, final @Nullable Object value, final long writeTime) {
		values.add(i, value);
		writeTimes.add(i, writeTime);
		expiries.add(i, NEVER);
	}

	void remove(final int i) {
		values.remove(i);
		writeTimes.remove(i);
		expiries.remove(i);
	}

	/**
	 * Writes a cell, unless a newer write already reached it.
	 *
	 * <p>Cassandra resolves two writes to the same cell by their timestamps, which is what makes
	 * {@code USING TIMESTAMP} more than a label: a write stamped older than the value already stored
	 * is discarded rather than applied. Equal timestamps are resolved here by taking the newer
	 * statement, where Cassandra compares the serialized values and keeps the greater.
	 *
	 * @return whether the write was applied
	 */
	boolean set(final int i, final @Nullable Object value, final long writeTime,
		final long expiresAt) {
		if (writeTime < writeTimes.get(i)) {
			return false;
		}
		values.set(i, value);
		writeTimes.set(i, writeTime);
		expiries.set(i, expiresAt);

		return true;
	}

	/**
	 * The value of a cell, or null when it holds none or has expired.
	 */
	@Nullable
	Object value(final int i, final long now) {
		return isLive(i, now) ? values.get(i) : null;
	}

	/**
	 * Whether a cell holds a value that has not expired. A cell holding null is not live, which is
	 * why {@code writetime()} of a null column answers null on a cluster.
	 */
	boolean isLive(final int i, final long now) {
		return values.get(i) != null && now < expiries.get(i);
	}

	/**
	 * The microsecond timestamp of a live cell, or null when the cell holds nothing.
	 */
	@Nullable
	Long writeTime(final int i, final long now) {
		return isLive(i, now) ? writeTimes.get(i) : null;
	}

	/**
	 * The seconds a live cell has left, or null when it holds nothing or was written without a TTL.
	 */
	@Nullable
	Integer ttl(final int i, final long now) {
		if (!isLive(i, now) || expiries.get(i) == NEVER) {
			return null;
		}

		return Math.toIntExact(expiries.get(i) - now);
	}

}
