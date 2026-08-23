package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SeaStarCqlSessionTest {

	@Test
	@DisplayName("A TypeChangeEvent evicts cached prepared statements referencing the changed UDT")
	void testPreparedStatementEvictedOnTypeChange() throws Exception {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			session.execute("CREATE KEYSPACE ks WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TYPE ks.address (street text, city text)");
			session.execute("CREATE TABLE ks.people (id int PRIMARY KEY, home frozen<address>)");

			// A standalone processor registers its own TypeChangeEvent listener on the shared event
			// bus, so we can observe its cache directly.
			final var processor = new SeaStarCqlPrepareAsyncProcessor(context);
			final var request = new DefaultPrepareRequest(
				"INSERT INTO ks.people (id, home) VALUES (?, ?)");
			final var prepared = processor.process(request, session, context, "test")
				.toCompletableFuture().get();

			// Hold a strong reference to the cached future so the weak-valued cache cannot drop it
			// before the event fires.
			final var cached = processor.getCache().getIfPresent(request);
			assertNotNull(cached);

			final var udt = (UserDefinedType) prepared.getVariableDefinitions().get("home").getType();
			((InternalDriverContext) context).getEventBus().fire(TypeChangeEvent.updated(udt, udt));

			assertNull(processor.getCache().getIfPresent(request));
		}
	}

	/**
	 * Not shared with {@link ContainerCqlSessionTest}: the cache under test is SeaStar's own, and a
	 * live cluster keeps prepared statements current through the protocol's result metadata id
	 * instead. Unlike the UDT test above, the event is not fired by hand - {@code ALTER TABLE} is
	 * what has to announce it.
	 */
	@Test
	@DisplayName("ALTER TABLE evicts cached prepared statements naming the altered table")
	void testPreparedStatementEvictedOnTableChange() throws Exception {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			session.execute("CREATE KEYSPACE ks WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE ks.people (id int PRIMARY KEY, name text)");

			final var processor = new SeaStarCqlPrepareAsyncProcessor(context);
			final var request = new DefaultPrepareRequest("SELECT * FROM ks.people WHERE id = ?");
			processor.process(request, session, context, "test").toCompletableFuture().get();

			// Hold a strong reference so the weak-valued cache cannot drop it before the event.
			final var cached = processor.getCache().getIfPresent(request);
			assertNotNull(cached);

			session.execute("ALTER TABLE ks.people ADD nickname text");

			assertNull(processor.getCache().getIfPresent(request));
		}
	}

	/**
	 * Not shared with {@link ContainerCqlSessionTest} because the digest is a node's internal scheme
	 * rather than anything the driver API promises - but both of these were captured from
	 * {@code cassandra:5.0.8}, which answers with exactly these bytes.
	 */
	@Test
	@DisplayName("A prepared statement's id is an MD5 of the keyspace and the query, as a node's is")
	void testPreparedStatementIdIsAnMd5() throws Exception {
		try (final var session = SeaStarCqlSession.builder().build()) {
			session.execute("CREATE KEYSPACE ks WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v text)");
			final var digest = MessageDigest.getInstance("MD5");

			final var qualified = "SELECT * FROM ks.t WHERE id = ?";
			assertEquals(ByteBuffer.wrap(digest.digest(qualified.getBytes(StandardCharsets.UTF_8))),
				session.prepare(qualified).getId());

			session.execute("USE ks");
			final var unqualified = "SELECT v FROM t WHERE id = ?";

			assertEquals(
				ByteBuffer.wrap(digest.digest(("ks" + unqualified).getBytes(StandardCharsets.UTF_8))),
				session.prepare(unqualified).getId());
		}
	}

	/**
	 * Not shared with {@link ContainerCqlSessionTest}: a live cluster does have a token ring, so it
	 * answers this with a populated {@link com.datastax.oss.driver.api.core.metadata.TokenMap}.
	 */
	@Test
	@DisplayName("getTokenMap is empty because SeaStar models a single node with no token ring")
	void testTokenMapIsEmpty() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			assertTrue(session.getMetadata().getTokenMap().isEmpty());
		}
	}

	/**
	 * Not shared with the fidelity suite: what a real driver answers here depends on its own state
	 * tracking, where SeaStar's answer is simply when the session came up.
	 */
	@Test
	@DisplayName("The node reports the session's start as its up-since time")
	void testUpSinceMillis() {
		final var before = System.currentTimeMillis();
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var node = session.getMetadata().getNodes().values().iterator().next();

			assertTrue(node.getUpSinceMillis() >= before);
			assertTrue(node.getUpSinceMillis() <= System.currentTimeMillis());
		}
	}

	@Test
	@DisplayName("A keyspace created outside CQL reports Cassandra's default replication")
	void testProgrammaticKeyspaceDefaults() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var keyspace = session.getContext().newSeaStarKeyspace("direct");

			assertEquals(
				Map.of("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor",
					"1"), keyspace.getReplication());
			assertTrue(keyspace.isDurableWrites());
		}
	}

	/**
	 * Not shared with {@link ContainerCqlSessionTest}: a live cluster runs every one of these, so the
	 * container would fail the assertions. The divergence is deliberate and documented in
	 * {@code docs/support-matrix.md}; what is under test is that each one names the feature SeaStar
	 * does not implement and quotes the query, rather than failing as an internal error.
	 */
	@Test
	@DisplayName("An unimplemented feature is rejected by name, with the query, as an invalid query")
	void testUnsupportedStatementsAreRejectedByName() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			session.execute("CREATE KEYSPACE foo WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE foo.t (pk int PRIMARY KEY, v text)");

			Map.ofEntries(Map.entry("CREATE MATERIALIZED VIEW foo.mv AS SELECT * FROM foo.t "
						+ "WHERE pk IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, pk)",
					"materialized views"),
				Map.entry("ALTER MATERIALIZED VIEW foo.mv WITH comment = 'x'", "materialized views"),
				Map.entry("DROP MATERIALIZED VIEW foo.mv", "materialized views"),
				Map.entry("CREATE FUNCTION foo.f(a int) RETURNS NULL ON NULL INPUT RETURNS int "
					+ "LANGUAGE java AS 'return a;'", "user-defined functions"),
				Map.entry("DROP FUNCTION foo.f", "user-defined functions"),
				Map.entry("CREATE AGGREGATE foo.agg(int) SFUNC f STYPE int",
					"user-defined aggregates"),
				Map.entry("DROP AGGREGATE foo.agg", "user-defined aggregates"),
				Map.entry("CREATE TRIGGER trg ON foo.t USING 'org.example.Trigger'", "triggers"),
				Map.entry("DROP TRIGGER trg ON foo.t", "triggers"),
				Map.entry("DESCRIBE TABLE foo.t", "DESCRIBE"),
				Map.entry("DESCRIBE KEYSPACES", "DESCRIBE"))
				.forEach((cql, feature) -> {
					final var thrown = assertThrows(InvalidQueryException.class,
						() -> session.execute(cql), cql);
					assertTrue(thrown.getMessage().contains(feature),
						"%s should name %s but said: %s".formatted(cql, feature,
							thrown.getMessage()));
					assertTrue(thrown.getMessage().contains(cql),
						"%s should quote the query but said: %s".formatted(cql,
							thrown.getMessage()));
					assertNotNull(thrown.getExecutionInfo(),
						"%s should carry execution info".formatted(cql));
				});

			// The auth statements are UnauthorizedException rather than InvalidQueryException,
			// matching a default node; see UnsupportedStatements.
			List.of("CREATE ROLE r", "DROP ROLE r", "GRANT SELECT ON KEYSPACE foo TO r",
					"REVOKE SELECT ON KEYSPACE foo FROM r", "LIST ROLES", "LIST ALL PERMISSIONS")
				.forEach(cql -> {
					final var thrown = assertThrows(UnauthorizedException.class,
						() -> session.execute(cql), cql);
					assertTrue(thrown.getMessage().contains("roles and permissions"),
						"%s should name roles and permissions but said: %s".formatted(cql,
							thrown.getMessage()));
					assertTrue(thrown.getMessage().contains(cql),
						"%s should quote the query but said: %s".formatted(cql,
							thrown.getMessage()));
					assertNotNull(thrown.getExecutionInfo(),
						"%s should carry execution info".formatted(cql));
				});
		}
	}

	/**
	 * Not shared with {@link ContainerCqlSessionTest}: a cluster's TTL expires when the wall clock
	 * says so, and a test that waited for it would have to sleep. SeaStar evaluates expiry on read
	 * against the clock the session was built with, so the test moves time instead.
	 */
	@Test
	@DisplayName("Advancing the clock expires a TTL without the test waiting for it")
	void testTtlExpiresAgainstTheClock() {
		final var clock = SeaStarClock.now();
		try (final var session = SeaStarCqlSession.builder().withClock(clock).build()) {
			session.execute("CREATE KEYSPACE ks WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v text, w text)");
			session.execute("INSERT INTO ks.t (pk, v, w) VALUES (1, 'a', 'b') USING TTL 60");
			session.execute("INSERT INTO ks.t (pk, v, w) VALUES (2, 'a', 'b')");
			session.execute("UPDATE ks.t USING TTL 60 SET v = 'x' WHERE pk = 2");

			assertEquals(2, session.execute("SELECT pk FROM ks.t").all().size());
			assertEquals(60, session.execute("SELECT ttl(v) FROM ks.t WHERE pk = 1").one().getInt(0));

			clock.advance(Duration.ofSeconds(30));
			assertEquals(30, session.execute("SELECT ttl(v) FROM ks.t WHERE pk = 1").one().getInt(0));

			clock.advance(Duration.ofSeconds(31));
			// Row 1 was inserted with the TTL, so its marker went with its columns and the row is gone.
			// Row 2 was inserted without one and only had a column re-written, so the row stays and
			// only that column is empty.
			final var rows = session.execute("SELECT pk, v, w FROM ks.t").all();
			assertEquals(1, rows.size());
			assertEquals(2, rows.get(0).getInt("pk"));
			assertNull(rows.get(0).getString("v"));
			assertEquals("b", rows.get(0).getString("w"));
			assertTrue(session.execute("SELECT ttl(v) FROM ks.t WHERE pk = 2").one().isNull(0));
			assertEquals(0L, session.execute("SELECT count(*) FROM ks.t WHERE pk = 1")
				.one()
				.getLong(0));
		}
	}

	/**
	 * Not shared with {@link ContainerCqlSessionTest}: a live cluster answers every one of these, so
	 * the container would fail the assertions. Each is a clause SeaStar used to accept and drop on the
	 * floor, which is the one failure mode that turns a green test into a false negative - the
	 * statement ran, the clause did nothing, and the answer was wrong. The divergence is deliberate
	 * and documented in {@code docs/support-matrix.md}.
	 */
	@Test
	@DisplayName("A clause SeaStar cannot honour is refused rather than ignored")
	void testUnsupportedClausesAreRejected() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			session.execute("CREATE KEYSPACE ks WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE ks.t (pk int, ck int, v text, m map<text, int>, "
				+ "PRIMARY KEY (pk, ck))");

			Map.ofEntries(
					Map.entry("SELECT count(*) FROM ks.t GROUP BY pk", "GROUP BY"),
					Map.entry("SELECT * FROM ks.t PER PARTITION LIMIT 1", "PER PARTITION LIMIT"),
					Map.entry("BEGIN BATCH USING TIMESTAMP 1 INSERT INTO ks.t (pk, ck) VALUES (1, 1); "
						+ "APPLY BATCH", "USING"),
					Map.entry("CREATE INDEX ON ks.t (v) USING 'org.example.Index'", "custom"),
					Map.entry("CREATE INDEX ON ks.t (v) WITH OPTIONS = {'a': 'b'}", "options"),
					Map.entry("CREATE INDEX ON ks.t (KEYS(m))", "keys index target"),
					Map.entry("SELECT m['a'] FROM ks.t", "element"),
					Map.entry("SELECT pk + 1 FROM ks.t", "SELECT clause"))
				.forEach((cql, named) -> {
					final var thrown = assertThrows(InvalidQueryException.class,
						() -> session.execute(cql), cql);
					assertTrue(thrown.getMessage().contains(named),
						"%s should name %s but said: %s".formatted(cql, named, thrown.getMessage()));
				});
		}
	}

	/**
	 * Not shared with {@link ContainerCqlSessionTest}: a real cluster does page, so it answers
	 * {@code hasMorePages()} with true and {@code fetchNextPage()} with a page. SeaStar's rows are
	 * already in this process, so a page boundary would be an invention; every row comes back on the
	 * first page and {@code setPageSize} is accepted and has no effect. The shared suite pins the part
	 * that has to agree - that the paging idioms terminate and return everything.
	 */
	@Test
	@DisplayName("Every row comes back on one page, whatever the page size")
	void testEverythingIsOnOnePage() throws Exception {
		try (final var session = SeaStarCqlSession.builder().build()) {
			session.execute("CREATE KEYSPACE ks WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE ks.t (pk int, ck int, PRIMARY KEY (pk, ck))");
			for (int i = 1; i <= 5; i++) {
				session.execute("INSERT INTO ks.t (pk, ck) VALUES (1, %d)".formatted(i));
			}

			final var statement = SimpleStatement.newInstance("SELECT ck FROM ks.t WHERE pk = 1")
				.setPageSize(2);
			final var page = session.executeAsync(statement).toCompletableFuture().get();

			assertEquals(5, page.remaining());
			assertFalse(page.hasMorePages());
			assertThrows(IllegalStateException.class, page::fetchNextPage);
		}
	}

}
