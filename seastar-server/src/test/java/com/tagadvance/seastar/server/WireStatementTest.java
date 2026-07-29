package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * c_plan C1, C4, C5, C6 and C7 driven off the wire: what a client sends, and the message it gets
 * back, with no {@code CqlSession} in the way.
 *
 * <p>Every expectation about a real node's shape - which statement answers with which message,
 * which error code, and the wording of the paging-state refusal - was captured from a
 * {@code cassandra:5.0.8} container over the same raw socket rather than reasoned about.
 */
class WireStatementTest {

	private static final int V4 = ProtocolConstants.Version.V4;
	private static final String CREATE_KEYSPACE = "CREATE KEYSPACE ks WITH replication = "
		+ "{'class':'SimpleStrategy','replication_factor':1}";

	private final AtomicInteger streamIds = new AtomicInteger();

	private SeaStarCqlSession session;
	private SeaStarProtocolServer server;
	private WireClient client;

	@BeforeEach
	void setUp() throws IOException {
		session = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(session).build().start();
		client = new WireClient(server.port());
		client.send(V4, streamIds.incrementAndGet(), new Startup());
	}

	@AfterEach
	void tearDown() throws IOException {
		client.close();
		server.close();
		session.close();
	}

	@Test
	@DisplayName("a SELECT comes back as ROWS carrying its column metadata")
	void testSelect() throws IOException {
		schema();
		send("INSERT INTO ks.t (id, v) VALUES (1, 'a')");

		final var rows = assertInstanceOf(Rows.class, send("SELECT * FROM ks.t"));
		final var specs = rows.getMetadata().columnSpecs;

		assertEquals(List.of("id", "v"), specs.stream().map(spec -> spec.name).toList());
		assertEquals("ks", specs.get(0).ksName);
		assertEquals("t", specs.get(0).tableName);
		assertEquals(1, rows.getData().size());
		assertNull(rows.getMetadata().pagingState);
	}

	@Test
	@DisplayName("a SELECT that matches nothing is ROWS with metadata and no rows, never VOID")
	void testEmptySelect() throws IOException {
		schema();

		final var rows = assertInstanceOf(Rows.class, send("SELECT * FROM ks.t"));

		assertEquals(2, rows.getMetadata().columnSpecs.size());
		assertEquals(0, rows.getData().size());
	}

	@Test
	@DisplayName("a SELECT is answered twice with the same values, not once")
	void testSelectTwice() throws IOException {
		schema();
		send("INSERT INTO ks.t (id, v) VALUES (1, 'first')");

		final var first = assertInstanceOf(Rows.class, send("SELECT v FROM ks.t"));
		final var second = assertInstanceOf(Rows.class, send("SELECT v FROM ks.t"));

		// Every value buffer is duplicated on the way out. Handing the same instance to the frame
		// encoder twice would leave the second read empty, which is what this pins.
		assertEquals("first", text(first.getData().peek().get(0)));
		assertEquals("first", text(second.getData().peek().get(0)));
	}

	@Test
	@DisplayName("a modification comes back as VOID")
	void testModificationIsVoid() throws IOException {
		schema();

		assertInstanceOf(Void.class, send("INSERT INTO ks.t (id, v) VALUES (1, 'a')"));
		assertInstanceOf(Void.class, send("UPDATE ks.t SET v = 'b' WHERE id = 1"));
		assertInstanceOf(Void.class, send("DELETE FROM ks.t WHERE id = 1"));
		assertInstanceOf(Void.class, send("TRUNCATE ks.t"));
	}

	@Test
	@DisplayName("a keyspace statement comes back as a SCHEMA_CHANGE naming the keyspace")
	void testKeyspaceSchemaChanges() throws IOException {
		assertSchemaChange(send(CREATE_KEYSPACE), "CREATED", "KEYSPACE", "ks", null);
		assertSchemaChange(send("ALTER KEYSPACE ks WITH durable_writes = false"), "UPDATED",
			"KEYSPACE", "ks", null);
		assertSchemaChange(send("DROP KEYSPACE ks"), "DROPPED", "KEYSPACE", "ks", null);
	}

	@Test
	@DisplayName("a table statement comes back as a SCHEMA_CHANGE naming the table")
	void testTableSchemaChanges() throws IOException {
		send(CREATE_KEYSPACE);

		assertSchemaChange(send("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"), "CREATED",
			"TABLE", "ks", "t");
		assertSchemaChange(send("ALTER TABLE ks.t ADD w text"), "UPDATED", "TABLE", "ks", "t");
		assertSchemaChange(send("DROP TABLE ks.t"), "DROPPED", "TABLE", "ks", "t");
	}

	@Test
	@DisplayName("a type statement comes back as a SCHEMA_CHANGE naming the type")
	void testTypeSchemaChanges() throws IOException {
		send(CREATE_KEYSPACE);

		assertSchemaChange(send("CREATE TYPE ks.address (street text)"), "CREATED", "TYPE", "ks",
			"address");
		assertSchemaChange(send("ALTER TYPE ks.address ADD zip int"), "UPDATED", "TYPE", "ks",
			"address");
		assertSchemaChange(send("DROP TYPE ks.address"), "DROPPED", "TYPE", "ks", "address");
	}

	@Test
	@DisplayName("an index statement comes back as an update to the table it indexes")
	void testIndexSchemaChanges() throws IOException {
		schema();

		// Not a target of its own: a driver tracks the table, so a node reports the table.
		assertSchemaChange(send("CREATE INDEX t_v_idx ON ks.t (v)"), "UPDATED", "TABLE", "ks", "t");
		assertSchemaChange(send("DROP INDEX ks.t_v_idx"), "UPDATED", "TABLE", "ks", "t");
	}

	@Test
	@DisplayName("dropping an index that is not there names no table and changes nothing")
	void testDroppingAnIndexThatIsNotThere() throws IOException {
		schema();

		assertInstanceOf(Void.class, send("DROP INDEX IF EXISTS ks.nope"));
	}

	@Test
	@DisplayName("an unqualified DDL statement resolves against the connection's keyspace")
	void testUnqualifiedSchemaChange() throws IOException {
		send(CREATE_KEYSPACE);
		send("USE ks");

		assertSchemaChange(send("CREATE TABLE t (id int PRIMARY KEY)"), "CREATED", "TABLE", "ks",
			"t");
	}

	@Test
	@DisplayName("USE comes back as SET_KEYSPACE and steers the statements after it")
	void testUse() throws IOException {
		schema();

		final var response = assertInstanceOf(SetKeyspace.class, send("USE ks"));
		assertEquals("ks", response.keyspace);

		assertInstanceOf(Void.class, send("INSERT INTO t (id, v) VALUES (1, 'a')"));
		assertInstanceOf(Rows.class, send("SELECT * FROM t"));
	}

	@Test
	@DisplayName("USE naming a keyspace that does not exist leaves the connection where it was")
	void testUseSomethingMissing() throws IOException {
		schema();
		send("USE ks");

		assertEquals(ProtocolConstants.ErrorCode.INVALID, error(send("USE nope")).code);
		assertInstanceOf(Rows.class, send("SELECT * FROM t"));
	}

	@Test
	@DisplayName("the keyspace is per connection: one that never ran USE has none")
	void testKeyspaceIsPerConnection() throws IOException {
		schema();
		send("USE ks");

		try (final var other = new WireClient(server.port())) {
			other.send(V4, 1, new Startup());
			final var response = other.send(V4, 2, new Query("SELECT * FROM t")).message;

			assertTrue(error(response).message.contains("No keyspace has been specified"),
				error(response).message);
			assertInstanceOf(Rows.class,
				other.send(V4, 3, new Query("SELECT * FROM ks.t")).message);
		}
	}

	@Test
	@DisplayName("a connection remembers a keyspace name, so dropping and recreating it recovers")
	void testTheKeyspaceIsRememberedByName() throws IOException {
		schema();
		send("USE ks");
		send("DROP KEYSPACE ks");

		// What cassandra:5.0.8 does: the connection still points at the name, so an unqualified
		// statement fails on the keyspace while a qualified one elsewhere is unaffected.
		assertTrue(error(send("SELECT * FROM t")).message.contains("keyspace ks does not exist"),
			error(send("SELECT * FROM t")).message);

		send(CREATE_KEYSPACE);
		send("CREATE TABLE ks.t (id int PRIMARY KEY, v text)");

		assertInstanceOf(Rows.class, send("SELECT * FROM t"));
	}

	@Test
	@DisplayName("a lightweight transaction comes back as ROWS with an [applied] column")
	void testLightweightTransaction() throws IOException {
		schema();

		final var applied = assertInstanceOf(Rows.class,
			send("INSERT INTO ks.t (id, v) VALUES (1, 'a') IF NOT EXISTS"));
		// A real node sends the literal name, brackets and all, unquoted.
		assertEquals(List.of("[applied]"),
			applied.getMetadata().columnSpecs.stream().map(spec -> spec.name).toList());
		assertEquals(ProtocolConstants.DataType.BOOLEAN,
			applied.getMetadata().columnSpecs.get(0).type.id);

		final var rejected = assertInstanceOf(Rows.class,
			send("INSERT INTO ks.t (id, v) VALUES (1, 'b') IF NOT EXISTS"));

		// The conflicting row's values follow [applied] when the condition did not hold.
		assertEquals(List.of("[applied]", "id", "v"),
			rejected.getMetadata().columnSpecs.stream().map(spec -> spec.name).toList());
	}

	@Test
	@DisplayName("an unknown table is INVALID, not a server error")
	void testUnknownTable() throws IOException {
		schema();

		final var error = error(send("SELECT * FROM ks.nope"));

		assertEquals(ProtocolConstants.ErrorCode.INVALID, error.code);
		assertTrue(error.message.contains("nope"), error.message);
	}

	@Test
	@DisplayName("a query that will not parse is SYNTAX_ERROR")
	void testSyntaxError() throws IOException {
		assertEquals(ProtocolConstants.ErrorCode.SYNTAX_ERROR, error(send("SELEC")).code);
	}

	@Test
	@DisplayName("creating something twice is ALREADY_EXISTS, naming the keyspace and the object")
	void testAlreadyExists() throws IOException {
		schema();

		final var keyspace = assertInstanceOf(AlreadyExists.class, send(CREATE_KEYSPACE));
		assertEquals(ProtocolConstants.ErrorCode.ALREADY_EXISTS, keyspace.code);
		assertEquals("ks", keyspace.keyspace);
		// A keyspace-level clash carries an empty table, which is how the driver tells the two apart.
		assertEquals("", keyspace.table);

		final var table = assertInstanceOf(AlreadyExists.class,
			send("CREATE TABLE ks.t (id int PRIMARY KEY)"));
		assertEquals("ks", table.keyspace);
		assertEquals("t", table.table);
	}

	@Test
	@DisplayName("a statement SeaStar does not implement arrives named, as INVALID")
	void testUnimplementedStatement() throws IOException {
		schema();

		final var error = error(send("CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t "
			+ "WHERE id IS NOT NULL PRIMARY KEY (id)"));

		// The whole point of C5: a harness has to see "SeaStar does not do this", not a broken server.
		assertEquals(ProtocolConstants.ErrorCode.INVALID, error.code);
		assertTrue(error.message.contains("materialized views"), error.message);
		assertTrue(error.message.contains("CREATE MATERIALIZED VIEW"), error.message);
	}

	@Test
	@DisplayName("a paging state is refused rather than ignored, since none was ever issued")
	void testPagingStateIsRefused() throws IOException {
		schema();
		final var options = options(List.of(), Map.of(), ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));

		final var error = error(send(new Query("SELECT * FROM ks.t", options)));

		// Ignoring it would answer page one forever, which is an infinite loop in the client. The
		// code and the wording are what cassandra:5.0.8 sends for a paging state it cannot read.
		assertEquals(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, error.code);
		assertEquals("Invalid value for the paging state", error.message);
	}

	@Test
	@DisplayName("a page size is accepted and ignored, because every answer is one page")
	void testPageSizeIsIgnored() throws IOException {
		schema();
		send("INSERT INTO ks.t (id, v) VALUES (1, 'a')");
		send("INSERT INTO ks.t (id, v) VALUES (2, 'b')");
		final var options = new QueryOptions(ProtocolConstants.ConsistencyLevel.ONE, List.of(),
			Map.of(), false, 1, null, ProtocolConstants.ConsistencyLevel.SERIAL,
			QueryOptions.NO_DEFAULT_TIMESTAMP, null, QueryOptions.NO_NOW_IN_SECONDS);

		final var rows = assertInstanceOf(Rows.class,
			send(new Query("SELECT * FROM ks.t", options)));

		assertEquals(2, rows.getData().size());
		assertNull(rows.getMetadata().pagingState);
	}

	@Test
	@DisplayName("positional values on a QUERY are bound by index")
	void testPositionalValues() throws IOException {
		schema();
		final var options = options(List.of(bytes(1), text("a")), Map.of(), null);

		assertInstanceOf(Void.class,
			send(new Query("INSERT INTO ks.t (id, v) VALUES (?, ?)", options)));
		assertEquals("a", text(row("SELECT v FROM ks.t").get(0)));
	}

	@Test
	@DisplayName("named values on a QUERY are bound by name")
	void testNamedValues() throws IOException {
		schema();
		final var options = options(List.of(), Map.of("id", bytes(1), "v", text("named")), null);

		assertInstanceOf(Void.class,
			send(new Query("INSERT INTO ks.t (id, v) VALUES (:id, :v)", options)));
		assertEquals("named", text(row("SELECT v FROM ks.t").get(0)));
	}

	@Test
	@DisplayName("a null bound value is bound as null, not skipped")
	void testNullValue() throws IOException {
		schema();
		final var options = options(Arrays.asList(bytes(1), null), Map.of(), null);

		assertInstanceOf(Void.class,
			send(new Query("INSERT INTO ks.t (id, v) VALUES (?, ?)", options)));
		assertNull(row("SELECT v FROM ks.t").get(0));
	}

	@Test
	@DisplayName("the wrong number of values is INVALID rather than a server error")
	void testWrongNumberOfValues() throws IOException {
		schema();
		final var options = options(List.of(bytes(1)), Map.of(), null);

		final var error = error(send(new Query("INSERT INTO ks.t (id, v) VALUES (?, ?)", options)));

		assertEquals(ProtocolConstants.ErrorCode.INVALID, error.code);
	}

	@Test
	@DisplayName("PREPARE answers with the bind variables and the result columns")
	void testPrepare() throws IOException {
		schema();

		final var insert = assertInstanceOf(Prepared.class,
			send(new Prepare("INSERT INTO ks.t (id, v) VALUES (?, ?)")));
		assertEquals(List.of("id", "v"),
			insert.variablesMetadata.columnSpecs.stream().map(spec -> spec.name).toList());
		assertArrayEquals(new int[]{0}, insert.variablesMetadata.pkIndices);
		// An INSERT returns nothing, and a node describes that as no metadata rather than no columns.
		assertEquals(0, insert.resultMetadata.columnCount);
		// resultMetadataId is a v5 field, and a v4 node sends none.
		assertNull(insert.resultMetadataId);

		final var select = assertInstanceOf(Prepared.class,
			send(new Prepare("SELECT * FROM ks.t WHERE id = ?")));
		assertEquals(List.of("id", "v"),
			select.resultMetadata.columnSpecs.stream().map(spec -> spec.name).toList());
	}

	@Test
	@DisplayName("EXECUTE runs the statement PREPARE handed an id for")
	void testExecute() throws IOException {
		schema();
		final var prepared = assertInstanceOf(Prepared.class,
			send(new Prepare("INSERT INTO ks.t (id, v) VALUES (?, ?)")));

		final var response = send(new Execute(prepared.preparedQueryId,
			options(List.of(bytes(1), text("bound")), Map.of(), null)));

		assertInstanceOf(Void.class, response);
		assertEquals("bound", text(row("SELECT v FROM ks.t").get(0)));
	}

	@Test
	@DisplayName("preparing the same query twice hands back the same id")
	void testPreparingTwice() throws IOException {
		schema();
		final var query = "SELECT * FROM ks.t WHERE id = ?";

		assertArrayEquals(
			assertInstanceOf(Prepared.class, send(new Prepare(query))).preparedQueryId,
			assertInstanceOf(Prepared.class, send(new Prepare(query))).preparedQueryId);
	}

	@Test
	@DisplayName("an id prepared on one connection executes on another, because it is the node's")
	void testAPreparedIdIsPerServer() throws IOException {
		schema();
		final var prepared = assertInstanceOf(Prepared.class,
			send(new Prepare("INSERT INTO ks.t (id, v) VALUES (?, ?)")));

		try (final var other = new WireClient(server.port())) {
			other.send(V4, 1, new Startup());
			final var execute = new Execute(prepared.preparedQueryId,
				options(List.of(bytes(1), text("elsewhere")), Map.of(), null));

			// A driver prepares on one pooled connection and executes on whichever the pool hands it.
			// An id that only worked where it was made would produce intermittent UNPREPARED rather
			// than a clean failure.
			assertInstanceOf(Void.class, other.send(V4, 2, execute).message);
		}

		assertEquals("elsewhere", text(row("SELECT v FROM ks.t").get(0)));
	}

	@Test
	@DisplayName("an id stays good however many statements are prepared after it")
	void testNothingIsEvicted() throws IOException {
		schema();
		final var first = assertInstanceOf(Prepared.class,
			send(new Prepare("INSERT INTO ks.t (id, v) VALUES (?, ?)")));
		for (int i = 0; i < 256; i++) {
			send(new Prepare("SELECT * FROM ks.t WHERE id = " + i));
		}

		// The registry is a plain map on purpose. A cache that silently dropped an id the client
		// still holds would answer UNPREPARED for something this server did issue.
		assertInstanceOf(Void.class, send(new Execute(first.preparedQueryId,
			options(List.of(bytes(1), text("still good")), Map.of(), null))));
	}

	@Test
	@DisplayName("a prepared SELECT run after ALTER TABLE answers with the column that was added")
	void testPreparedStatementAfterASchemaChange() throws IOException {
		schema();
		send("INSERT INTO ks.t (id, v) VALUES (1, 'a')");
		final var prepared = assertInstanceOf(Prepared.class,
			send(new Prepare("SELECT * FROM ks.t WHERE id = ?")));
		assertEquals(List.of("id", "v"),
			prepared.resultMetadata.columnSpecs.stream().map(spec -> spec.name).toList());

		send("ALTER TABLE ks.t ADD w text");
		final var rows = assertInstanceOf(Rows.class, send(new Execute(prepared.preparedQueryId,
			options(List.of(bytes(1)), Map.of(), null))));

		// e_plan E4: the registry holds a core prepared statement rather than a parse result of its
		// own, and the core re-resolves the query every time it runs, so there is nothing here to
		// subscribe to SchemaChanges. A v5 server would also have to change the resultMetadataId; v4
		// has no such mechanism, and the driver takes what the response describes.
		assertEquals(List.of("id", "v", "w"),
			rows.getMetadata().columnSpecs.stream().map(spec -> spec.name).toList());
	}

	@Test
	@DisplayName("EXECUTE on an id this server never issued is UNPREPARED, carrying the id back")
	void testUnprepared() throws IOException {
		final var id = new byte[]{1, 2, 3, 4};

		final var response = assertInstanceOf(Unprepared.class,
			send(new Execute(id, QueryOptions.DEFAULT)));

		assertEquals(ProtocolConstants.ErrorCode.UNPREPARED, response.code);
		assertArrayEquals(id, response.id);
	}

	@Test
	@DisplayName("a BATCH applies its children, whether they are query strings or prepared ids")
	void testBatch() throws IOException {
		schema();
		final var prepared = assertInstanceOf(Prepared.class,
			send(new Prepare("INSERT INTO ks.t (id, v) VALUES (?, ?)")));
		final var batch = new Batch(ProtocolConstants.BatchType.LOGGED,
			List.of(prepared.preparedQueryId, "INSERT INTO ks.t (id, v) VALUES (2, 'plain')"),
			List.of(List.of(bytes(1), text("prepared")), List.of()),
			ProtocolConstants.ConsistencyLevel.ONE, ProtocolConstants.ConsistencyLevel.SERIAL,
			QueryOptions.NO_DEFAULT_TIMESTAMP, null, QueryOptions.NO_NOW_IN_SECONDS);

		assertInstanceOf(Void.class, send(batch));

		final var rows = assertInstanceOf(Rows.class, send("SELECT v FROM ks.t"));
		assertEquals(2, rows.getData().size());
	}

	@Test
	@DisplayName("a BATCH naming an id this server never issued is UNPREPARED")
	void testBatchWithAnUnknownId() throws IOException {
		schema();
		final var batch = new Batch(ProtocolConstants.BatchType.LOGGED,
			List.of((Object) new byte[]{9, 9}), List.of(List.of()),
			ProtocolConstants.ConsistencyLevel.ONE, ProtocolConstants.ConsistencyLevel.SERIAL,
			QueryOptions.NO_DEFAULT_TIMESTAMP, null, QueryOptions.NO_NOW_IN_SECONDS);

		assertInstanceOf(Unprepared.class, send(batch));
	}

	@Test
	@DisplayName("a BATCH naming an unknown id applies nothing, not the statements before it")
	void testBatchWithAnUnknownIdAppliesNothing() throws IOException {
		schema();
		final var batch = new Batch(ProtocolConstants.BatchType.LOGGED,
			List.<Object>of("INSERT INTO ks.t (id, v) VALUES (1, 'first')", new byte[]{9, 9}),
			List.of(List.of(), List.of()), ProtocolConstants.ConsistencyLevel.ONE,
			ProtocolConstants.ConsistencyLevel.SERIAL, QueryOptions.NO_DEFAULT_TIMESTAMP, null,
			QueryOptions.NO_NOW_IN_SECONDS);

		assertInstanceOf(Unprepared.class, send(batch));

		// The whole batch is assembled before any of it runs. Executing the prefix and then failing
		// would leave the client re-preparing and retrying a batch whose first half already applied.
		assertEquals(0, assertInstanceOf(Rows.class, send("SELECT * FROM ks.t")).getData().size());
	}

	private void schema() throws IOException {
		send(CREATE_KEYSPACE);
		send("CREATE TABLE ks.t (id int PRIMARY KEY, v text)");
	}

	private Message send(final String cql) throws IOException {
		return send(new Query(cql));
	}

	private Message send(final Message request) throws IOException {
		return client.send(V4, streamIds.incrementAndGet() & 0x7FFF, request).message;
	}

	private List<ByteBuffer> row(final String cql) throws IOException {
		return assertInstanceOf(Rows.class, send(cql)).getData().peek();
	}

	private static QueryOptions options(final List<ByteBuffer> positional,
		final Map<String, ByteBuffer> named, final ByteBuffer pagingState) {
		return new QueryOptions(ProtocolConstants.ConsistencyLevel.ONE, positional, named, false, -1,
			pagingState, ProtocolConstants.ConsistencyLevel.SERIAL,
			QueryOptions.NO_DEFAULT_TIMESTAMP, null, QueryOptions.NO_NOW_IN_SECONDS);
	}

	private static void assertSchemaChange(final Message message, final String change,
		final String target, final String keyspace, final String object) {
		final var response = assertInstanceOf(SchemaChange.class, message);

		assertEquals(change, response.changeType);
		assertEquals(target, response.target);
		assertEquals(keyspace, response.keyspace);
		assertEquals(object, response.object);
	}

	private static Error error(final Message message) {
		return assertInstanceOf(Error.class, message);
	}

	private static ByteBuffer bytes(final int value) {
		return ByteBuffer.allocate(Integer.BYTES).putInt(0, value);
	}

	private static ByteBuffer text(final String value) {
		return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(final ByteBuffer value) {
		return StandardCharsets.UTF_8.decode(value.duplicate()).toString();
	}


}
