package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
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

}
