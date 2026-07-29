package com.tagadvance.seastar.handlers;

import com.tagadvance.seastar.SeaStarDriverContext;
import java.time.Clock;

/**
 * When a statement's writes happened and when they stop being readable, which is what {@code USING
 * TIMESTAMP} and {@code USING TTL} say and what {@code writetime()} and {@code ttl()} report back.
 *
 * <p>Both are resolved once per statement, against the session's clock, so every cell one statement
 * writes carries the same stamp - as it does on a cluster, where the coordinator picks one timestamp
 * for the whole mutation.
 *
 * @param timestamp when the write happened, in microseconds since the epoch
 * @param expiresAt when the written cells stop being readable, in seconds since the epoch, or
 *                  {@link #NEVER} for a statement with no TTL
 */
record Writes(long timestamp, long expiresAt) {

	/**
	 * The expiry of a cell written without a TTL.
	 */
	static final long NEVER = Long.MAX_VALUE;

	static Writes of(final SeaStarDriverContext context, final Modification modification) {
		final var clock = context.getClock();
		final var timestamp = modification.timestamp() == null ? microseconds(clock)
			: modification.timestamp();
		final var ttl = modification.ttl();
		// A TTL of zero is how CQL spells "no TTL", which is also what a column keeps when a statement
		// that names one overwrites a column that had one.
		final var expiresAt = ttl == null || ttl == 0 ? NEVER : seconds(clock) + ttl;

		return new Writes(timestamp, expiresAt);
	}

	/**
	 * The write time to stamp a cell the statement did not name, which is every cell of a row it
	 * creates: it carries the statement's timestamp but never its TTL, because only a named column
	 * expires.
	 */
	Writes withoutExpiry() {
		return expiresAt == NEVER ? this : new Writes(timestamp, NEVER);
	}

	static long microseconds(final Clock clock) {
		final var instant = clock.instant();

		return Math.multiplyExact(instant.getEpochSecond(), 1_000_000L) + instant.getNano() / 1_000;
	}

	static long seconds(final Clock clock) {
		return clock.instant().getEpochSecond();
	}

}
