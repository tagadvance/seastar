package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The lock model, exercised rather than described. A live cluster cannot reach SeaStar's own locks,
 * so this is a unit test rather than part of the two-backend suite.
 *
 * <p><strong>Every test here is bounded and none of them may hang.</strong> Worker threads are
 * daemons and nothing is joined without a timeout, so a lock cycle fails the test on a latch rather
 * than parking a Gradle test worker forever - which is the worse of the two outcomes by a distance.
 */
class ConcurrencyTest {

	/**
	 * How long a stuck thread is given before the test calls it stuck. Generous: the point is to
	 * separate "deadlocked" from "slow on a loaded machine", not to measure anything.
	 */
	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private static final ThreadFactory DAEMONS = runnable -> {
		final var thread = new Thread(runnable, "seastar-concurrency-test");
		thread.setDaemon(true);

		return thread;
	};

	private SeaStarCqlSession session;

	@BeforeEach
	void beforeEach() {
		session = SeaStarCqlSession.builder().build();
	}

	@AfterEach
	void afterEach() {
		session.close();
	}

	/**
	 * The regression test for the deadlock b_plan B1 reproduced. Three threads were enough: one
	 * reading a row under the table's read lock, one writing a column by name - which took the row's
	 * write lock and then the table's read lock, inverting the order - and one wanting the table's
	 * write lock, which a write-preferring {@link java.util.concurrent.locks.ReentrantReadWriteLock}
	 * puts in front of the second thread's read.
	 *
	 * <p>The JVM does not report the cycle through {@code findDeadlockedThreads}, because an AQS lock
	 * order cycle is not a monitor cycle. It presents as a suite that never finishes, which is why
	 * this is written to fail rather than to hang.
	 */
	@Test
	@DisplayName("A row written by column name while the table is read and written never deadlocks")
	void byNameWriteInvertsNoLockOrder() throws InterruptedException {
		final var table = session.getContext().newSeaStarKeyspace("b1").newSeaStarTable("t");
		table.addColumn("id", DataTypes.INT);
		table.addColumn("name", DataTypes.TEXT);
		table.markPartitionKey(CqlIdentifier.fromInternal("id"));
		final var row = table.addRow(1, "a");

		assertCompletes(List.of(() -> row.snapshot(), () -> row.set("name", "x"),
			() -> table.writeLock(() -> {
			})));
	}

	/**
	 * The same shape one level up, because a keyspace lock is now what a table hands out: a thread
	 * reading a table's metadata, one adding a column, and one walking the keyspace's table map.
	 */
	@Test
	@DisplayName("Reading a table's metadata while its columns change never deadlocks")
	void schemaReadsAndWritesInterleave() throws InterruptedException {
		final var keyspace = session.getContext().newSeaStarKeyspace("b1b");
		final var table = keyspace.newSeaStarTable("t");
		table.addColumn("id", DataTypes.INT);
		table.markPartitionKey(CqlIdentifier.fromInternal("id"));
		final var counter = new java.util.concurrent.atomic.AtomicInteger();

		assertCompletes(List.of(table::getColumns, table::snapshot,
			() -> table.insertColumn(CqlIdentifier.fromInternal("c" + counter.getAndIncrement()),
				DataTypes.TEXT, false), keyspace::getSeaStarTables));
	}

	/**
	 * DDL against DML, which is the dimension that finds anything: the tree already survives a mixed
	 * insert, select and update load, because those all take the same lock. Dropping and recreating a
	 * table underneath them is what makes the keyspace lock earn its keep.
	 *
	 * <p>Every statement here can legitimately fail - a SELECT from a table a competing thread has
	 * just dropped is an {@code InvalidQueryException}, not a bug - so the assertion is on the
	 * threads finishing and on nothing escaping that a caller could not have caused.
	 */
	@Test
	@DisplayName("Mixed DDL, DML and SELECT against one session complete without an internal failure")
	void mixedDdlAndDmlSurviveEachOther() throws InterruptedException {
		session.execute("CREATE KEYSPACE stress WITH REPLICATION = "
			+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
		session.execute("CREATE TABLE stress.t (id int PRIMARY KEY, name text)");

		final Queue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
		final var id = new java.util.concurrent.atomic.AtomicInteger();
		final List<Runnable> work = List.of(
			() -> tolerate(unexpected,
				() -> session.execute("INSERT INTO stress.t (id, name) VALUES (%d, 'a')".formatted(
					id.getAndIncrement() % 64))),
			() -> tolerate(unexpected,
				() -> session.execute("UPDATE stress.t SET name = 'b' WHERE id = %d".formatted(
					id.get() % 64))),
			() -> tolerate(unexpected,
				() -> session.execute("DELETE FROM stress.t WHERE id = %d".formatted(id.get() % 64))),
			() -> tolerate(unexpected, () -> session.execute("SELECT * FROM stress.t")),
			() -> tolerate(unexpected,
				() -> session.execute("SELECT name FROM stress.t WHERE id = %d".formatted(
					id.get() % 64))),
			// The DDL dimension. Both are legal against a table another thread is reading.
			() -> tolerate(unexpected, () -> session.execute(
				"CREATE TABLE IF NOT EXISTS stress.u (id int PRIMARY KEY, name text)")),
			() -> tolerate(unexpected, () -> session.execute("DROP TABLE IF EXISTS stress.u")),
			() -> tolerate(unexpected, () -> session.getMetadata().getKeyspaces()),
			() -> tolerate(unexpected, () -> session.getMetadata()
				.getKeyspace("stress")
				.map(keyspace -> keyspace.getTables().size())
				.orElse(0)));

		assertCompletes(work);
		assertEquals(List.of(), List.copyOf(unexpected),
			"a statement failed with something a caller could not have caused");
	}

	/**
	 * A driver exception is a legitimate answer here - a statement racing a DROP TABLE gets one - so
	 * only the failures no client could have provoked are collected.
	 */
	private static void tolerate(final Queue<Throwable> unexpected, final Runnable runnable) {
		try {
			runnable.run();
		} catch (final com.datastax.oss.driver.api.core.DriverException e) {
			// The race answered, which is the point.
		} catch (final Throwable e) {
			unexpected.add(e);
		}
	}

	/**
	 * Runs each task on its own daemon thread, in a loop, until every one of them has managed a fair
	 * number of passes, and asserts that they all finished.
	 *
	 * <p>The assertion is the whole test: a lock cycle means one of these threads never returns, the
	 * latch times out, and the test goes red instead of the build going quiet.
	 */
	private static void assertCompletes(final List<Runnable> tasks) throws InterruptedException {
		final var iterations = 2_000;
		final var running = new AtomicBoolean(true);
		final var ready = new CountDownLatch(tasks.size());
		final var done = new CountDownLatch(tasks.size());
		final Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
		final ExecutorService executor = Executors.newFixedThreadPool(tasks.size(), DAEMONS);
		try {
			tasks.forEach(task -> executor.execute(() -> {
				ready.countDown();
				try {
					IntStream.range(0, iterations)
						.takeWhile(ignored -> running.get())
						.forEach(ignored -> task.run());
				} catch (final Throwable e) {
					failures.add(e);
				} finally {
					done.countDown();
				}
			}));

			assertTrue(ready.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS),
				"the worker threads never started");
			final var finished = done.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			running.set(false);

			assertEquals(List.of(), List.copyOf(failures), "a worker thread failed");
			assertTrue(finished, "the worker threads did not finish within %s: they are deadlocked"
				.formatted(TIMEOUT));
		} finally {
			// Never awaitTermination: a parked thread would take the timeout with it, and these are
			// daemons precisely so a stuck one cannot outlive the JVM.
			running.set(false);
			executor.shutdownNow();
		}
	}

}
