package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * The session itself: keyspace selection and how a statement resolves through it, the execution
 * info and errors a request comes back with, and the session lifecycle. This group owns the
 * {@code sess}, {@code idks} and {@code closing} keyspaces.
 */
public abstract class AbstractSessionFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("sess");
	}

	/**
	 * Runs before the {@code USE} at order 2 because it has to: the digest omits the keyspace only
	 * while none is selected, and no statement puts a session back in that state.
	 *
	 * @see #testPreparedStatementIdDigestsTheKeyspace()
	 */
	@Test
	@Order(0)
	@DisplayName("With no keyspace selected a prepared statement is identified by MD5 of the query")
	void testPreparedStatementIdWithoutAKeyspace() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS idks WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		session.execute("CREATE TABLE IF NOT EXISTS idks.t (id uuid PRIMARY KEY, v text)");
		assertTrue(session.getKeyspace().isEmpty(), "no keyspace may be selected yet");

		final var query = "SELECT * FROM idks.t WHERE id = ?";

		assertEquals(md5(query), hex(session.prepare(query).getId()));
	}

	@Test
	@Order(2)
	@DisplayName("USE selects the keyspace, quoted or unquoted, and getKeyspace reports it")
	void testUseKeyspace() {
		Stream.of("USE sess", "USE \"sess\";").forEach(cql -> {
			final var resultSet = assertDoesNotThrow(() -> session.execute(cql));
			assertNotNull(resultSet);
		});

		final var keyspace = session.getKeyspace();
		assertTrue(keyspace.isPresent());
		assertEquals("sess", keyspace.get().asInternal());
	}

	@Test
	@Order(67)
	@DisplayName("A rethrown DriverException carries execution info")
	void testDriverExceptionCarriesExecutionInfo() {
		session.execute("CREATE TABLE IF NOT EXISTS sess.t (id int PRIMARY KEY, name text)");

		final var error = assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT nope FROM sess.t"));

		final var executionInfo = error.getExecutionInfo();
		assertNotNull(executionInfo);
		assertEquals("SELECT nope FROM sess.t",
			((SimpleStatement) executionInfo.getRequest()).getQuery());
		assertTrue(executionInfo.getWarnings().isEmpty());
		assertTrue(executionInfo.isSchemaInAgreement());
	}

	@Test
	@Order(81)
	@DisplayName("An unqualified statement resolves its table against the session keyspace")
	void testUnqualifiedStatementsUseSessionKeyspace() {
		session.execute("USE sess");
		session.execute("CREATE TABLE IF NOT EXISTS sess.unqualified (id int PRIMARY KEY, name text)");

		session.execute("INSERT INTO unqualified (id, name) VALUES (1, 'inserted')");
		final var inserted = session.execute("SELECT * FROM unqualified").all();
		assertEquals(1, inserted.size());
		assertEquals("inserted", inserted.get(0).getString("name"));

		session.execute("UPDATE unqualified SET name = 'updated' WHERE id = 1");
		assertEquals("updated",
			session.execute("SELECT name FROM unqualified WHERE id = 1").one().getString("name"));

		session.execute("DELETE FROM unqualified WHERE id = 1");
		assertNull(session.execute("SELECT name FROM unqualified WHERE id = 1").one());

		session.execute("TRUNCATE unqualified");
	}

	@Test
	@Order(82)
	@DisplayName("An unqualified prepared statement exposes its variable and result definitions")
	void testUnqualifiedPreparedMetadata() {
		session.execute("USE sess");
		session.execute(
			"CREATE TABLE IF NOT EXISTS sess.unqualified_prepared (id int PRIMARY KEY, name text)");

		final var prepared = session.prepare("SELECT name FROM unqualified_prepared WHERE id = ?");

		final var variables = prepared.getVariableDefinitions();
		assertEquals(1, variables.size());
		assertEquals("id", variables.get(0).getName().asInternal());
		assertEquals(DataTypes.INT, variables.get(0).getType());

		final var result = prepared.getResultSetDefinitions();
		assertEquals(1, result.size());
		assertTrue(result.contains("name"));
	}

	@Test
	@Order(83)
	@DisplayName("A statement naming an unknown keyspace throws InvalidQueryException")
	void testUnknownKeyspace() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT * FROM no_such_keyspace.people"));
		assertThrows(InvalidQueryException.class, () -> session.execute(
			"INSERT INTO no_such_keyspace.people (id, name) VALUES (1, 'x')"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("UPDATE no_such_keyspace.people SET name = 'x' WHERE id = 1"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("DELETE FROM no_such_keyspace.people WHERE id = 1"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("TRUNCATE no_such_keyspace.people"));
	}

	@Test
	@Order(84)
	@DisplayName("ExecutionInfo answers its routine getters instead of throwing")
	void testExecutionInfoRoutineGetters() {
		session.execute("CREATE TABLE IF NOT EXISTS sess.exec_info (id int PRIMARY KEY)");

		final var executionInfo = session.execute("SELECT * FROM sess.exec_info")
			.getExecutionInfo();

		assertNull(executionInfo.getPagingState());
		assertNull(executionInfo.getSafePagingState());
		assertTrue(executionInfo.getIncomingPayload().isEmpty());
		assertNull(executionInfo.getTracingId());

		final var error = assertThrows(IllegalStateException.class, executionInfo::getQueryTrace);
		assertEquals("Tracing was disabled for this request", error.getMessage());

		final var stage = executionInfo.getQueryTraceAsync().toCompletableFuture();
		final var asyncError = assertThrows(CompletionException.class, stage::join);
		assertInstanceOf(IllegalStateException.class, asyncError.getCause());
	}

	@Test
	@Order(85)
	@DisplayName("getMetrics is empty because metrics are disabled")
	void testGetMetricsIsEmpty() {
		assertTrue(session.getMetrics().isEmpty());
	}

	@Test
	@Order(87)
	@DisplayName("A closed session rejects further requests and completes its close future")
	void testClosedSessionRejectsRequests() {
		final var doomed = createInstance();
		doomed.execute("CREATE KEYSPACE IF NOT EXISTS closing WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		doomed.execute("CREATE TABLE IF NOT EXISTS closing.t (id int PRIMARY KEY)");

		assertFalse(doomed.closeFuture().toCompletableFuture().isDone());

		doomed.close();

		assertTrue(doomed.closeFuture().toCompletableFuture().isDone());
		assertDoesNotThrow(doomed::close);

		final var syncError = assertThrows(IllegalStateException.class,
			() -> doomed.execute("SELECT * FROM closing.t"));
		assertEquals("Session is closed", syncError.getMessage());

		final var prepareError = assertThrows(IllegalStateException.class,
			() -> doomed.prepare("SELECT * FROM closing.t WHERE id = ?"));
		assertEquals("Session is closed", prepareError.getMessage());

		final var stage = doomed.executeAsync("SELECT * FROM closing.t").toCompletableFuture();
		final var asyncError = assertThrows(CompletionException.class, stage::join);
		assertInstanceOf(IllegalStateException.class, asyncError.getCause());
		assertEquals("Session is closed", asyncError.getCause().getMessage());

		// Metadata outlives the close: the driver keeps its last schema snapshot readable, and
		// SeaStar keeps the model it was serving.
		final var keyspace = doomed.getMetadata().getKeyspace("closing");
		assertTrue(keyspace.isPresent());
		assertTrue(keyspace.orElseThrow().getTable("t").isPresent());
	}

	@Test
	@Order(95)
	@DisplayName("executeAsync reports every failure as a failed stage, never as a throw")
	void testExecuteAsyncNeverThrows() {
		session.execute("CREATE TABLE IF NOT EXISTS sess.async_fail (id int PRIMARY KEY, name text)");

		final var queries = List.of("SELECT * FROM no_such_keyspace.t",
			"SELECT * FROM sess.no_such_table", "SELECT no_such_col FROM sess.async_fail",
			"INSERT INTO no_such_keyspace.t (id) VALUES (1)", "SELECT FROM WHERE",
			"CREATE MATERIALIZED VIEW sess.mv AS SELECT * FROM sess.async_fail "
				+ "WHERE id IS NOT NULL PRIMARY KEY (id)");

		for (final var query : queries) {
			final var stage = assertDoesNotThrow(() -> session.executeAsync(query),
				"executeAsync must not throw for: " + query);

			final var async = assertThrows(CompletionException.class,
				() -> stage.toCompletableFuture().join(), "stage must fail for: " + query);
			final var sync = assertThrows(RuntimeException.class, () -> session.execute(query),
				"execute must throw for: " + query);

			assertEquals(sync.getClass(), async.getCause().getClass(),
				"execute and executeAsync must agree for: " + query);
		}
	}

	@Test
	@Order(147)
	@DisplayName("USE on a keyspace that does not exist names it and leaves the session where it was")
	void testUseMissingKeyspace() {
		session.execute("USE sess");

		assertMentions("nope", assertThrows(InvalidQueryException.class,
			() -> session.execute("USE nope")));
		assertEquals(CqlIdentifier.fromInternal("sess"), session.getKeyspace().orElseThrow());
	}

	/**
	 * The half of the rule that is easy to get half right: the selected keyspace is digested ahead
	 * of the query, so the same query prepared under two keyspaces has two ids. The query is
	 * deliberately unqualified, so it can only resolve through the selected keyspace.
	 *
	 * @see #testPreparedStatementIdWithoutAKeyspace()
	 */
	@Test
	@Order(148)
	@DisplayName("A prepared statement's id digests the selected keyspace ahead of the query")
	void testPreparedStatementIdDigestsTheKeyspace() {
		session.execute("USE sess");
		session.execute("CREATE TABLE IF NOT EXISTS ids (id uuid PRIMARY KEY, v text)");
		final var keyspace = session.getKeyspace()
			.orElseThrow(() -> new IllegalStateException("a keyspace is required to digest one"));

		final var query = "SELECT v FROM ids WHERE id = ?";

		assertEquals(md5(keyspace.asInternal() + query), hex(session.prepare(query).getId()));
	}

	@Test
	@Order(251)
	@DisplayName("The node reports Cassandra 5.0.8, the release SeaStar borrows its behavior from")
	void testCassandraVersion() {
		final var node = session.getMetadata().getNodes().values().iterator().next();

		assertEquals(Version.parse("5.0.8"), node.getCassandraVersion());
	}

	@Test
	@Order(248)
	@DisplayName("Auth statements are refused with UnauthorizedException, as on a node without auth")
	void testAuthStatementsUnauthorized() {
		// Type only: a default node's wording ("You have to be logged in...") is its own, and so
		// is SeaStar's. The message assertions live in SeaStarCqlSessionTest.
		Stream.of("CREATE ROLE r", "DROP ROLE r", "GRANT SELECT ON KEYSPACE sess TO r",
				"REVOKE SELECT ON KEYSPACE sess FROM r", "LIST ROLES", "LIST ALL PERMISSIONS")
			.forEach(cql -> assertThrows(UnauthorizedException.class, () -> session.execute(cql),
				cql));
	}

}
