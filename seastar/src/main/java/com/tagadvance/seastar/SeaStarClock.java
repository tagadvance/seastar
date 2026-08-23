package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import net.jcip.annotations.ThreadSafe;

/**
 * A {@link Clock} a test moves by hand, so that expiry can be observed without waiting for it.
 *
 * <p>SeaStar evaluates a TTL lazily, on read, against the clock the session was built with. Give a
 * session one of these and a row written with {@code USING TTL 60} expires the moment the test says
 * it does:
 *
 * <pre>{@code
 * final var clock = SeaStarClock.now();
 * try (final var session = SeaStarCqlSession.builder().withClock(clock).build()) {
 *     session.execute("INSERT INTO ks.t (id, v) VALUES (1, 'a') USING TTL 60");
 *     clock.advance(Duration.ofSeconds(61));
 *     // the row is gone
 * }
 * }</pre>
 *
 * <p>The default is {@link Clock#systemUTC()}, so a session that is not given one behaves like a
 * cluster: a TTL expires when the wall clock says so.
 */
@ThreadSafe
public final class SeaStarClock extends Clock {

	private final AtomicReference<Instant> instant;
	private final ZoneId zone;

	private SeaStarClock(final AtomicReference<Instant> instant, final ZoneId zone) {
		this.instant = requireNonNull(instant, "instant must not be null");
		this.zone = requireNonNull(zone, "zone must not be null");
	}

	/**
	 * A clock started at the current time and stopped there until it is advanced.
	 */
	public static SeaStarClock now() {
		return at(Instant.now());
	}

	/**
	 * A clock started at the given instant and stopped there until it is advanced.
	 */
	public static SeaStarClock at(final Instant instant) {
		requireNonNull(instant, "instant must not be null");

		return new SeaStarClock(new AtomicReference<>(instant), ZoneOffset.UTC);
	}

	/**
	 * Moves the clock forward. A negative duration moves it back.
	 */
	public void advance(final Duration duration) {
		requireNonNull(duration, "duration must not be null");

		instant.updateAndGet(current -> current.plus(duration));
	}

	@Override
	public ZoneId getZone() {
		return zone;
	}

	/**
	 * The copy shares this clock's instant rather than snapshotting it: advancing either clock moves
	 * both, and only the zone differs.
	 */
	@Override
	public Clock withZone(final ZoneId zone) {
		// Shares the instant, so a view in another zone still moves when this clock is advanced.
		return new SeaStarClock(instant, zone);
	}

	@Override
	public Instant instant() {
		return instant.get();
	}

	@Override
	public String toString() {
		return "SeaStarClock[%s]".formatted(instant.get());
	}

}
