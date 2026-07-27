package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SeaStarCqlSessionTest extends AbstractCqlSessionTest {

	@Override
	protected CqlSession createInstance() {
		return SeaStarCqlSession.builder().build();
	}

	@Test
	@DisplayName("A TypeChangeEvent evicts cached prepared statements referencing the changed UDT")
	void testPreparedStatementEvictedOnTypeChange() throws Exception {
		try (final var session = (SeaStarCqlSession) SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			session.execute("CREATE KEYSPACE ks WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TYPE ks.address (street text, city text)");
			session.execute("CREATE TABLE ks.people (id int PRIMARY KEY, home frozen<address>)");

			// A standalone processor registers its own TypeChangeEvent listener on the shared event
			// bus, so we can observe its cache directly.
			final var processor = new SeaStarCqlPrepareAsyncProcessor(Optional.of(context));
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

			final var processor = new SeaStarCqlPrepareAsyncProcessor(Optional.of(context));
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
	 * Not shared with {@link ContainerCqlSessionTest}: a real cluster owns the schema independently
	 * of the session, so its metadata stays readable after close. SeaStar's storage <em>is</em> the
	 * session, so closing discards it and a leaked session fails loudly.
	 */
	@Test
	@DisplayName("Closing a session discards its keyspaces")
	void testCloseDiscardsKeyspaces() {
		final var session = (SeaStarCqlSession) SeaStarCqlSession.builder().build();
		session.execute("CREATE KEYSPACE ks WITH replication = "
			+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
		assertTrue(session.getContext().getSeaStarKeyspace("ks").isPresent());

		session.close();

		assertTrue(session.getContext().getSeaStarKeyspaces().isEmpty());
		assertTrue(session.getMetadata().getKeyspaces().isEmpty());
	}

	@Test
	@DisplayName("A keyspace created outside CQL reports Cassandra's default replication")
	void testProgrammaticKeyspaceDefaults() {
		try (final var session = (SeaStarCqlSession) SeaStarCqlSession.builder().build()) {
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
				Map.entry("DESCRIBE KEYSPACES", "DESCRIBE"),
				Map.entry("CREATE ROLE r", "roles and permissions"),
				Map.entry("DROP ROLE r", "roles and permissions"),
				Map.entry("GRANT SELECT ON KEYSPACE foo TO r", "roles and permissions"),
				Map.entry("REVOKE SELECT ON KEYSPACE foo FROM r", "roles and permissions"),
				Map.entry("LIST ROLES", "roles and permissions"),
				Map.entry("LIST ALL PERMISSIONS", "roles and permissions"))
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
		}
	}

}
