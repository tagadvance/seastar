package com.tagadvance.seastar.handlers;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.handlers.CqlStatementSummary.Change;
import com.tagadvance.seastar.handlers.CqlStatementSummary.KeyspaceSelected;
import com.tagadvance.seastar.handlers.CqlStatementSummary.Result;
import com.tagadvance.seastar.handlers.CqlStatementSummary.SchemaChanged;
import com.tagadvance.seastar.handlers.CqlStatementSummary.Target;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What a statement is, in the terms a server has to answer it in.
 *
 * <p>The expectations are the result messages a {@code cassandra:5.0.8} node sends for the same
 * statements, taken off a v4 socket: {@code SCHEMA_CHANGE} for DDL, {@code SET_KEYSPACE} for
 * {@code USE}, and - for creating or dropping an index - an update to the table that owns it rather
 * than a target of its own.
 */
class CqlStatementSummaryTest {

	private SeaStarCqlSession session;

	@BeforeEach
	void setUp() {
		session = SeaStarCqlSession.builder().build();
		session.execute("CREATE KEYSPACE ks WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}");
		session.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v text)");
		session.execute("CREATE INDEX t_v_idx ON ks.t (v)");
	}

	@AfterEach
	void tearDown() {
		session.close();
	}

	@Test
	@DisplayName("a statement that only reads or writes rows is neither of the special cases")
	void testPlainStatements() {
		assertInstanceOf(Result.class, summarize("SELECT * FROM ks.t"));
		assertInstanceOf(Result.class, summarize("INSERT INTO ks.t (id) VALUES (1)"));
		assertInstanceOf(Result.class, summarize("UPDATE ks.t SET v = 'a' WHERE id = 1"));
		assertInstanceOf(Result.class, summarize("DELETE FROM ks.t WHERE id = 1"));
		assertInstanceOf(Result.class, summarize("TRUNCATE ks.t"));
	}

	@Test
	@DisplayName("USE names the keyspace it selects")
	void testUse() {
		final var summary = assertInstanceOf(KeyspaceSelected.class, summarize("USE ks"));

		assertEquals("ks", summary.keyspace());
	}

	@Test
	@DisplayName("a keyspace statement is a schema change with no object")
	void testKeyspaceStatements() {
		assertSchemaChange(summarize("CREATE KEYSPACE k2 WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}"), Change.CREATED, Target.KEYSPACE,
			"k2", null);
		assertSchemaChange(summarize("ALTER KEYSPACE ks WITH durable_writes = false"),
			Change.UPDATED, Target.KEYSPACE, "ks", null);
		assertSchemaChange(summarize("DROP KEYSPACE ks"), Change.DROPPED, Target.KEYSPACE, "ks",
			null);
	}

	@Test
	@DisplayName("a table statement names the table")
	void testTableStatements() {
		assertSchemaChange(summarize("CREATE TABLE ks.t2 (id int PRIMARY KEY)"), Change.CREATED,
			Target.TABLE, "ks", "t2");
		assertSchemaChange(summarize("ALTER TABLE ks.t ADD w text"), Change.UPDATED, Target.TABLE,
			"ks", "t");
		assertSchemaChange(summarize("DROP TABLE ks.t"), Change.DROPPED, Target.TABLE, "ks", "t");
	}

	@Test
	@DisplayName("a type statement names the type")
	void testTypeStatements() {
		assertSchemaChange(summarize("CREATE TYPE ks.address (street text)"), Change.CREATED,
			Target.TYPE, "ks", "address");
		assertSchemaChange(summarize("ALTER TYPE ks.address ADD zip int"), Change.UPDATED,
			Target.TYPE, "ks", "address");
		assertSchemaChange(summarize("DROP TYPE ks.address"), Change.DROPPED, Target.TYPE, "ks",
			"address");
	}

	@Test
	@DisplayName("an index statement is an update to the table it indexes")
	void testIndexStatements() {
		assertSchemaChange(summarize("CREATE INDEX ON ks.t (v)"), Change.UPDATED, Target.TABLE,
			"ks", "t");
		// DROP INDEX names only the index, so the table is found by looking through the keyspace -
		// which is why a statement has to be summarized before it runs.
		assertSchemaChange(summarize("DROP INDEX ks.t_v_idx"), Change.UPDATED, Target.TABLE, "ks",
			"t");
	}

	@Test
	@DisplayName("dropping an index that is not there names no table")
	void testDroppingAnIndexThatIsNotThere() {
		assertSchemaChange(summarize("DROP INDEX IF EXISTS ks.nope"), Change.UPDATED, Target.TABLE,
			"ks", null);
	}

	@Test
	@DisplayName("an unqualified statement resolves against the session keyspace")
	void testUnqualified() {
		assertSchemaChange(summarizeIn("ks", "CREATE TABLE t2 (id int PRIMARY KEY)"),
			Change.CREATED, Target.TABLE, "ks", "t2");
		assertSchemaChange(summarizeIn("ks", "DROP INDEX t_v_idx"), Change.UPDATED, Target.TABLE,
			"ks", "t");
	}

	@Test
	@DisplayName("a statement written with its own keyspace ignores the session's")
	void testQualifiedWinsOverTheSessionKeyspace() {
		assertSchemaChange(summarizeIn("other", "DROP TABLE ks.t"), Change.DROPPED, Target.TABLE,
			"ks", "t");
	}

	@Test
	@DisplayName("a DDL statement that would change nothing is still a schema change")
	void testNoOpDdlIsStillReported() {
		// A real node compares the schema before and after and answers VOID here. Over-reporting
		// costs a driver one redundant refresh; under-reporting would leave it stale, so this is
		// deliberate rather than an oversight.
		assertSchemaChange(summarize("CREATE TABLE IF NOT EXISTS ks.t (id int PRIMARY KEY)"),
			Change.CREATED, Target.TABLE, "ks", "t");
	}

	@Test
	@DisplayName("a query that will not parse fails here rather than being summarized as a result")
	void testSyntaxError() {
		assertThrows(SyntaxError.class, () -> summarize("SELEC"));
	}

	private CqlStatementSummary summarize(final String query) {
		return summarizeIn("ks", query);
	}

	private CqlStatementSummary summarizeIn(final String keyspace, final String query) {
		return CqlStatementSummary.of(session.getMetadata(), CqlIdentifier.fromInternal(keyspace),
			query);
	}

	private static void assertSchemaChange(final CqlStatementSummary summary, final Change change,
		final Target target, final String keyspace, final String object) {
		final var changed = assertInstanceOf(SchemaChanged.class, summary);

		assertEquals(change, changed.change());
		assertEquals(target, changed.target());
		assertEquals(keyspace, changed.keyspace());
		assertEquals(object, changed.object());
	}

}
