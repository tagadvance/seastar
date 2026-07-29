package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The property the funnel exists to provide: statements arriving on many connections at once leave
 * the model in the state they would have left it in one at a time.
 *
 * <p>{@code ConcurrencyTest} in the core covers the model's own locks and is unchanged by any of
 * this. What is new over the wire is that several Netty workers decode at once, so without the
 * single-threaded executor the session would be entered concurrently - which the model tolerates,
 * but which is not what the listener promises.
 *
 * <p>Every wait here is bounded and every failure is an assertion. {@code WireClient} sets a socket
 * timeout of its own, so a request that is never answered fails the test rather than parking a
 * Gradle worker forever.
 */
class ConcurrentConnectionTest {

	private static final int V4 = ProtocolConstants.Version.V4;

	private static final int CONNECTIONS = 8;

	private static final int WRITES_PER_CONNECTION = 25;

	private static final int TIMEOUT_SECONDS = 60;

	private SeaStarCqlSession session;

	private SeaStarProtocolServer server;

	@BeforeEach
	void setUp() {
		session = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(session).build().start();
		session.execute("CREATE KEYSPACE ks WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}");
	}

	@AfterEach
	void tearDown() {
		server.close();
		session.close();
	}

	@Test
	@DisplayName("overlapping writes from many connections all land, each exactly once")
	void testNoWriteIsLost() throws Exception {
		session.execute("CREATE TABLE ks.t (pk int, ck int, who int, PRIMARY KEY (pk, ck))");

		// One partition on purpose. Separate partitions are separate entries in the row map and would
		// hardly contend at all, which is the version of this test that passes without a funnel.
		inParallel((connection, client) -> {
			for (int i = 0; i < WRITES_PER_CONNECTION; i++) {
				final var ck = connection * WRITES_PER_CONNECTION + i;
				assertInstanceOf(Void.class,
					client.send(V4, connection, new Query(
						"INSERT INTO ks.t (pk, ck, who) VALUES (1, %d, %d)".formatted(ck,
							connection))).message);
			}

			return connection;
		});

		final var rows = session.execute("SELECT ck FROM ks.t WHERE pk = 1").all();
		assertEquals(CONNECTIONS * WRITES_PER_CONNECTION, rows.size(),
			"every write should have landed exactly once");
		assertEquals(IntStream.range(0, CONNECTIONS * WRITES_PER_CONNECTION).boxed().toList(),
			rows.stream().map(row -> row.getInt("ck")).toList());
	}

	@Test
	@DisplayName("only one connection of many wins a lightweight transaction on the same row")
	void testOneWinnerPerLightweightTransaction() throws Exception {
		session.execute("CREATE TABLE ks.claim (id int PRIMARY KEY, owner int)");

		// A read and a write that have to be one step. Two connections both finding the row absent is
		// exactly what a second thread inside the session would produce, and the model's own locks
		// would not prevent it - the condition and the write are separate acquisitions.
		final var applied = inParallel((connection, client) -> {
			final var rows = assertInstanceOf(Rows.class, client.send(V4, connection, new Query(
				"INSERT INTO ks.claim (id, owner) VALUES (0, %d) IF NOT EXISTS".formatted(
					connection))).message);
			final var first = rows.getData().poll();
			assertNotNull(first, "a lightweight transaction answers with an [applied] row");

			return first.get(0).get(0) != 0 ? 1 : 0;
		});

		assertEquals(1, applied.stream().mapToInt(Integer::intValue).sum(),
			"exactly one connection should have found the row absent");
		assertEquals(1, session.execute("SELECT owner FROM ks.claim").all().size());
	}

	/**
	 * Runs one task per connection, each on its own thread with its own socket, all released together
	 * so that they overlap rather than queue.
	 *
	 * @param <T>  what a task answers with
	 * @param task what a connection does, given its index and its socket
	 * @return each task's answer, in connection order
	 * @throws Exception if a task failed, or the run outlasted its budget
	 */
	private <T> List<T> inParallel(final ConnectionTask<T> task) throws Exception {
		final var clients = new ArrayList<WireClient>(CONNECTIONS);
		final var pool = Executors.newFixedThreadPool(CONNECTIONS);
		try {
			final var start = new CountDownLatch(CONNECTIONS);
			final var tasks = new ArrayList<Callable<T>>(CONNECTIONS);
			for (int i = 0; i < CONNECTIONS; i++) {
				final var connection = i;
				final var client = new WireClient(server.port());
				client.send(V4, 0, new Startup());
				clients.add(client);

				tasks.add(() -> {
					start.countDown();
					assertTrue(start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
						"every connection should have reached the start line");

					return task.run(connection, client);
				});
			}

			final var answers = new ArrayList<T>(CONNECTIONS);
			for (final var future : pool.invokeAll(tasks, TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				answers.add(future.get());
			}

			return answers;
		} finally {
			pool.shutdownNow();
			for (final var client : clients) {
				client.close();
			}
		}
	}

	@FunctionalInterface
	private interface ConnectionTask<T> {

		/**
		 * @param connection the connection's index, unique within the run
		 * @param client     that connection's socket
		 * @return whatever the caller wants collected
		 * @throws Exception if the work failed
		 */
		T run(int connection, WireClient client) throws Exception;
	}

}
