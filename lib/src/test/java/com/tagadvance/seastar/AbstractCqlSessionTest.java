package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
abstract class AbstractCqlSessionTest {

	protected abstract CqlSession createInstance();

	private CqlSession session;

	@BeforeEach
	void beforeEach() {
		if (session == null) {
			session = createInstance();
		}
	}

	@Test
	@Order(1)
	void testCreateKeyspace() {
		final var context = session.getContext();
		if (context instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(
				seaStarContext.getSeaStarKeyspace(CqlIdentifier.fromInternal("foo")).isEmpty());
		}

		final var resultSet1 = session.execute(
			"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		assertNotNull(resultSet1);

		final var resultSet2 = assertDoesNotThrow(() -> session.execute(
			"CREATE KEYSPACE IF NOT EXISTS foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));
		assertNotNull(resultSet2);

		assertThrows(AlreadyExistsException.class, () -> session.execute(
			"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));

		if (context instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(
				seaStarContext.getSeaStarKeyspace(CqlIdentifier.fromInternal("foo")).isPresent());
		}
	}

	@Test
	@Order(2)
	void testUseKeyspace() {
		Stream.of("USE foo", "USE \"foo\";").forEach(cql -> {
			final var resultSet = assertDoesNotThrow(() -> session.execute(cql));
			assertNotNull(resultSet);
		});

		final var keyspace = session.getKeyspace();
		assertTrue(keyspace.isPresent());
		assertEquals("foo", keyspace.get().asInternal());
	}

	@Test
	@Order(3)
	void testCreateSimpleTable() {
		final var context = session.getContext();

		final var resultSet1 = session.execute("""
			CREATE TABLE users (
			    user_id UUID PRIMARY KEY,
			    first_name text
			);""");
		assertNotNull(resultSet1);

		final var resultSet2 = assertDoesNotThrow(() -> session.execute("""
			CREATE TABLE IF NOT EXISTS users (
			    user_id UUID PRIMARY KEY,
			    first_name text
			);"""));
		assertNotNull(resultSet2);

		assertThrows(AlreadyExistsException.class, () -> session.execute("""
			CREATE TABLE users (
			    user_id UUID PRIMARY KEY,
			    first_name text
			);"""));

		if (context instanceof SeaStarDriverContext seaStarContext) {
			final var table = seaStarContext.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarTable("users"));
			assertTrue(table.isPresent());

			final var columns = table.get().getColumns();
			assertEquals(DataTypes.UUID,
				columns.get(CqlIdentifier.fromInternal("user_id")).getType());
			assertEquals(DataTypes.TEXT,
				columns.get(CqlIdentifier.fromInternal("first_name")).getType());

			assertEquals(List.of(CqlIdentifier.fromInternal("user_id")),
				table.get().getPartitionKey().stream().map(ColumnMetadata::getName).toList());
			assertTrue(table.get().getClusteringColumns().isEmpty());
		}
	}

	@Test
	@Order(4)
	@DisplayName("SELECT * returns all columns addressable by index, name, and identifier")
	void testSimpleSelect() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.bar (foo text PRIMARY KEY, bar text)");
		session.execute("INSERT INTO foo.bar (foo, bar) VALUES ('foo', 'bar')");

		final var resultSet = session.execute("SELECT * FROM foo.bar");
		assertNotNull(resultSet);
		assertTrue(resultSet.wasApplied());
		var all = resultSet.all();
		assertEquals(1, all.size());
		final Row row = all.get(0);
		assertEquals("foo", row.getString(0));
		assertEquals("foo", row.getString("foo"));
		assertEquals("foo", row.getString(CqlIdentifier.fromInternal("foo")));
		assertEquals("bar", row.getString(1));
		assertEquals("bar", row.getString("bar"));
		assertEquals("bar", row.getString(CqlIdentifier.fromInternal("bar")));
	}

	@Test
	@Order(5)
	void testCreateTableWithPrimaryKeyAndClusteringColumn() {
		final var context = session.getContext();

		final var resultSet1 = session.execute("""
			CREATE TABLE user_status_updates (
				user_id uuid,
				updated_at timestamp,
				status text,
				PRIMARY KEY (user_id, updated_at)
			);""");
		assertNotNull(resultSet1);

		if (context instanceof SeaStarDriverContext seaStarContext) {
			final var table = seaStarContext.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarTable("user_status_updates"));
			assertTrue(table.isPresent());

			assertEquals(List.of(CqlIdentifier.fromInternal("user_id")),
				table.get().getPartitionKey().stream().map(ColumnMetadata::getName).toList());

			final var clustering = table.get().getClusteringColumns();
			assertEquals(List.of(CqlIdentifier.fromInternal("updated_at")),
				clustering.keySet().stream().map(ColumnMetadata::getName).toList());
			assertEquals(ClusteringOrder.ASC,
				clustering.values().iterator().next());
		}
	}

	@Test
	@Order(6)
	void testCreateTableWithPrimaryKeysAndClusteringColumn() {
		final var context = session.getContext();

		final var resultSet1 = session.execute("""
			CREATE TABLE device_metrics (
				device_id uuid,
				log_date date,
				log_time time,
				metric_value double,
				PRIMARY KEY ((device_id, log_date), log_time)
			);""");
		assertNotNull(resultSet1);

		if (context instanceof SeaStarDriverContext seaStarContext) {
			final var table = seaStarContext.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarTable("device_metrics"));
			assertTrue(table.isPresent());

			assertEquals(
				List.of(CqlIdentifier.fromInternal("device_id"),
					CqlIdentifier.fromInternal("log_date")),
				table.get().getPartitionKey().stream().map(ColumnMetadata::getName).toList());
			assertEquals(List.of(CqlIdentifier.fromInternal("log_time")),
				table.get().getClusteringColumns().keySet().stream().map(ColumnMetadata::getName)
					.toList());
		}
	}

	@Test
	@Order(7)
	void testNewTableWithAllPossibleDataTypes() {
		final var context = session.getContext();

		final var resultSet1 = session.execute("""
			CREATE TYPE IF NOT EXISTS "foo".phone_profile (
				country_code int,
				phone_number text
			);""");
		assertNotNull(resultSet1);

		if (context instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(seaStarContext.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarUserDefinedType("phone_profile"))
				.isPresent());
		}

		final var resultSet2 = session.execute("""
			CREATE TABLE IF NOT EXISTS all_types_master (
			    -- Primary Key Fields (Required)
			    id uuid,
			    category text,
			
			    -- Text and Character Types
			    ascii_sample ascii,
			    varchar_sample varchar, -- Alias for text
			    text_sample text,
			
			    -- Numeric Types (Integers)
			    tinyint_sample tinyint,   -- 1-byte integer
			    smallint_sample smallint, -- 2-byte integer
			    int_sample int,           -- 4-byte integer
			    bigint_sample bigint,     -- 8-byte long
			    varint_sample varint,     -- Arbitrary-precision integer
			
			    -- Numeric Types (Floating point & Decimals)
			    float_sample float,       -- 32-bit IEEE float
			    double_sample double,     -- 64-bit IEEE float
			    decimal_sample decimal,   -- Variable-precision decimal
			
			    -- Date and Time Types
			    date_sample date,           -- Date without time (yyyy-mm-dd)
			    time_sample time,           -- Time without date (nanoseconds since midnight)
			    timestamp_sample timestamp, -- Date plus time (millisecond precision)
			    duration_sample duration,   -- Duration of time (months, days, nanoseconds)
			
			    -- Unique Identifiers
			    timeuuid_sample timeuuid, -- Type 1 UUID embedded with a timestamp
			
			    -- Binary/Miscellaneous Types
			    boolean_sample boolean,
			    blob_sample blob,         -- Arbitrary bytes / binary stream
			    inet_sample inet,         -- IPv4 or IPv6 address
			
			    -- Collection Types
			    list_sample list<text>,
			    set_sample set<int>,
			    map_sample map<text, text>,
			
			    -- Complex / Advanced Types
			    tuple_sample tuple<int, text, boolean>,
			    udt_sample frozen<phone_profile>,       -- Custom User-Defined Type
			    vector_sample vector<float, 3>,         -- 3D Vector array for AI embeddings
			
			    -- Defining the composite Primary Key
			    PRIMARY KEY ((id), category)
			);""");
		assertNotNull(resultSet2);

		if (context instanceof SeaStarDriverContext seaStarContext) {
			final var table = seaStarContext.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarTable("all_types_master"))
				.orElseThrow();
			final var columns = table.getColumns();

			assertEquals(DataTypes.listOf(DataTypes.TEXT, false),
				columns.get(CqlIdentifier.fromInternal("list_sample")).getType());
			assertEquals(DataTypes.setOf(DataTypes.INT, false),
				columns.get(CqlIdentifier.fromInternal("set_sample")).getType());
			assertEquals(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT, false),
				columns.get(CqlIdentifier.fromInternal("map_sample")).getType());
			assertEquals(DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT, DataTypes.BOOLEAN),
				columns.get(CqlIdentifier.fromInternal("tuple_sample")).getType());

			final var vector = (VectorType) columns.get(CqlIdentifier.fromInternal("vector_sample"))
				.getType();
			assertEquals(DataTypes.FLOAT, vector.getElementType());
			assertEquals(3, vector.getDimensions());

			final var udt = (UserDefinedType) columns.get(CqlIdentifier.fromInternal("udt_sample"))
				.getType();
			assertTrue(udt.isFrozen());
			assertEquals(CqlIdentifier.fromInternal("phone_profile"), udt.getName());
			assertEquals(List.of(CqlIdentifier.fromInternal("country_code"),
					CqlIdentifier.fromInternal("phone_number")), udt.getFieldNames());
			assertEquals(List.of(DataTypes.INT, DataTypes.TEXT), udt.getFieldTypes());
		}
	}

	@Test
	@Order(7)
	@DisplayName("CREATE TABLE referencing an undefined UDT throws InvalidQueryException")
	void testCreateTableWithUndefinedUdtThrows() {
		assertThrows(InvalidQueryException.class, () -> session.execute(
			"CREATE TABLE undefined_udt_ref (id uuid PRIMARY KEY, profile frozen<no_such_type>)"));
	}

	private static final UUID ANN_ID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
	private static final UUID BOB_ID = UUID.fromString("223e4567-e89b-12d3-a456-426614174001");
	private static final UUID CAROL_ID = UUID.fromString("323e4567-e89b-12d3-a456-426614174002");

	private String nameOf(final UUID id) {
		return session.execute("SELECT * FROM foo.people").all().stream()
			.filter(row -> id.equals(row.getUuid("id")))
			.map(row -> row.getString("name"))
			.reduce((a, b) -> {
				throw new AssertionError("More than one row for id " + id);
			})
			.orElse(null);
	}

	@Test
	@Order(8)
	@DisplayName("INSERT with bind markers stores a row readable by SELECT")
	void testInsertWithBindMarkers() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS foo.people (id uuid PRIMARY KEY, name text);");

		final var prepared = session.prepare(
			"INSERT INTO foo.people (id, name) VALUES (?, ?)");
		final var resultSet = session.execute(prepared.bind(ANN_ID, "Ann"));
		assertNotNull(resultSet);

		assertEquals("Ann", nameOf(ANN_ID));
	}

	@Test
	@Order(9)
	@DisplayName("INSERT with literal values stores a row readable by SELECT")
	void testInsertWithLiterals() {
		final var resultSet = session.execute(
			"INSERT INTO foo.people (id, name) VALUES (223e4567-e89b-12d3-a456-426614174001, 'Bob')");
		assertNotNull(resultSet);

		assertEquals("Bob", nameOf(BOB_ID));
	}

	@Test
	@Order(10)
	@DisplayName("INSERT IF NOT EXISTS does not overwrite an existing row")
	void testInsertIfNotExists() {
		session.execute(
			"INSERT INTO foo.people (id, name) VALUES (323e4567-e89b-12d3-a456-426614174002, 'Carol') IF NOT EXISTS");
		session.execute(
			"INSERT INTO foo.people (id, name) VALUES (323e4567-e89b-12d3-a456-426614174002, 'Dave') IF NOT EXISTS");

		assertEquals("Carol", nameOf(CAROL_ID));
	}

	@Test
	@Order(11)
	@DisplayName("INSERT into an unknown table throws InvalidQueryException")
	void testInsertUnknownTable() {
		assertThrows(InvalidQueryException.class, () -> session.execute(
			"INSERT INTO foo.nope (id, name) VALUES (323e4567-e89b-12d3-a456-426614174002, 'x')"));
	}

	@Test
	@Order(12)
	@DisplayName("INSERT omitting the primary key throws InvalidQueryException")
	void testInsertMissingPrimaryKey() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO foo.people (name) VALUES ('Ann')"));
	}

	@Test
	@Order(13)
	@DisplayName("SELECT with a column list projects only the selected columns")
	void testSelectProjection() {
		final var resultSet = session.execute("SELECT name FROM foo.people WHERE id = " + ANN_ID);

		final var definitions = resultSet.getColumnDefinitions();
		assertEquals(1, definitions.size());
		assertEquals("name", definitions.get(0).getName().asInternal());

		final var all = resultSet.all();
		assertEquals(1, all.size());
		assertEquals("Ann", all.get(0).getString("name"));
	}

	@Test
	@Order(14)
	@DisplayName("SELECT WHERE = on the partition key returns the matching row")
	void testSelectWhereEquals() {
		final var prepared = session.prepare("SELECT * FROM foo.people WHERE id = ?");
		final var all = session.execute(prepared.bind(BOB_ID)).all();

		assertEquals(1, all.size());
		assertEquals("Bob", all.get(0).getString("name"));
	}

	@Test
	@Order(15)
	@DisplayName("SELECT WHERE IN returns every matching row")
	void testSelectWhereIn() {
		final var prepared = session.prepare("SELECT * FROM foo.people WHERE id IN (?, ?)");
		final var names = session.execute(prepared.bind(ANN_ID, CAROL_ID)).all().stream()
			.map(row -> row.getString("name"))
			.sorted()
			.toList();

		assertEquals(List.of("Ann", "Carol"), names);
	}

	@Test
	@Order(16)
	@DisplayName("SELECT filtering a non-key column requires ALLOW FILTERING")
	void testSelectFilteringRequiresAllowFiltering() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT * FROM foo.people WHERE name = 'Ann'"));

		final var all = session.execute(
			"SELECT * FROM foo.people WHERE name = 'Ann' ALLOW FILTERING").all();
		assertEquals(1, all.size());
		assertEquals("Ann", all.get(0).getString("name"));
	}

	@Test
	@Order(17)
	@DisplayName("SELECT LIMIT caps the row count and rejects a non-positive limit")
	void testSelectLimit() {
		assertEquals(1, session.execute("SELECT * FROM foo.people LIMIT 1").all().size());

		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT * FROM foo.people LIMIT 0"));
	}

	@Test
	@Order(18)
	@DisplayName("SELECT of an unknown column throws InvalidQueryException")
	void testSelectUnknownColumn() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT nope FROM foo.people"));
	}

	private static final UUID EVE_ID = UUID.fromString("423e4567-e89b-12d3-a456-426614174003");

	@Test
	@Order(19)
	@DisplayName("UPDATE SET on an existing row changes the value")
	void testUpdateExistingRow() {
		final var prepared = session.prepare(
			"UPDATE foo.people SET name = ? WHERE id = ?");
		session.execute(prepared.bind("Annette", ANN_ID));

		assertEquals("Annette", nameOf(ANN_ID));
	}

	@Test
	@Order(20)
	@DisplayName("UPDATE of a non-existent primary key upserts a new row")
	void testUpdateUpsert() {
		assertNull(nameOf(EVE_ID));

		session.execute(
			"UPDATE foo.people SET name = 'Eve' WHERE id = 423e4567-e89b-12d3-a456-426614174003");

		assertEquals("Eve", nameOf(EVE_ID));
	}

	@Test
	@Order(21)
	@DisplayName("UPDATE setting a primary key column throws InvalidQueryException")
	void testUpdatePrimaryKeyInSet() {
		assertThrows(InvalidQueryException.class, () -> session.execute(
			"UPDATE foo.people SET id = " + BOB_ID + " WHERE id = " + ANN_ID));
	}

	@Test
	@Order(22)
	@DisplayName("UPDATE restricting a non-primary-key column throws InvalidQueryException")
	void testUpdateNonKeyWhere() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("UPDATE foo.people SET name = 'x' WHERE name = 'Bob'"));
	}

	private static final UUID FRANK_ID = UUID.fromString("523e4567-e89b-12d3-a456-426614174004");
	private static final UUID GRACE_ID = UUID.fromString("623e4567-e89b-12d3-a456-426614174005");

	@Test
	@Order(23)
	@DisplayName("DELETE by primary key removes only the matching row")
	void testDeleteByPrimaryKey() {
		session.execute("INSERT INTO foo.people (id, name) VALUES (" + FRANK_ID + ", 'Frank')");
		assertEquals("Frank", nameOf(FRANK_ID));

		final var prepared = session.prepare("DELETE FROM foo.people WHERE id = ?");
		session.execute(prepared.bind(FRANK_ID));

		assertNull(nameOf(FRANK_ID));
		assertEquals("Bob", nameOf(BOB_ID));
	}

	@Test
	@Order(24)
	@DisplayName("DELETE of a named column nulls it but keeps the row")
	void testDeleteColumn() {
		session.execute("INSERT INTO foo.people (id, name) VALUES (" + GRACE_ID + ", 'Grace')");

		session.execute("DELETE name FROM foo.people WHERE id = " + GRACE_ID);

		final var rows = session.execute("SELECT * FROM foo.people WHERE id = " + GRACE_ID).all();
		assertEquals(1, rows.size());
		assertNull(rows.get(0).getString("name"));
	}

	@Test
	@Order(25)
	@DisplayName("DELETE restricting a non-primary-key column throws InvalidQueryException")
	void testDeleteNonKeyWhere() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("DELETE FROM foo.people WHERE name = 'Bob'"));
	}

	@Test
	@Order(26)
	@DisplayName("DELETE naming a primary key column throws InvalidQueryException")
	void testDeletePrimaryKeyColumn() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("DELETE id FROM foo.people WHERE id = " + BOB_ID));
	}

	@Test
	@Order(27)
	@DisplayName("DELETE from an unknown table throws InvalidQueryException")
	void testDeleteUnknownTable() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("DELETE FROM foo.nope WHERE id = " + BOB_ID));
	}

	@Test
	@Order(28)
	@DisplayName("DROP TABLE removes the table; dropping it again throws unless IF EXISTS")
	void testDropTable() {
		session.execute(
			"CREATE TABLE foo.doomed (id uuid PRIMARY KEY, name text)");

		session.execute("DROP TABLE foo.doomed");

		if (session.getContext() instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(seaStarContext.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarTable("doomed")).isEmpty());
		}

		assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP TABLE foo.doomed"));

		assertDoesNotThrow(() -> session.execute("DROP TABLE IF EXISTS foo.doomed"));
	}

	@Test
	@Order(29)
	@DisplayName("DROP KEYSPACE removes the keyspace; dropping it again throws unless IF EXISTS")
	void testDropKeyspace() {
		session.execute(
			"CREATE KEYSPACE throwaway WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

		session.execute("DROP KEYSPACE throwaway");

		if (session.getContext() instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(seaStarContext.getSeaStarKeyspace("throwaway").isEmpty());
		}

		assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP KEYSPACE throwaway"));

		assertDoesNotThrow(() -> session.execute("DROP KEYSPACE IF EXISTS throwaway"));
	}

	@Test
	@Order(30)
	@DisplayName("TRUNCATE empties the table but keeps it; unknown table throws InvalidQueryException")
	void testTruncate() {
		session.execute("CREATE TABLE foo.temporary (id uuid PRIMARY KEY, name text)");
		session.execute(
			"INSERT INTO foo.temporary (id, name) VALUES (523e4567-e89b-12d3-a456-426614174004, 'Eve')");

		session.execute("TRUNCATE foo.temporary");

		assertEquals(0, session.execute("SELECT * FROM foo.temporary").all().size());

		if (session.getContext() instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(seaStarContext.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarTable("temporary")).isPresent());
		}

		assertThrows(InvalidQueryException.class,
			() -> session.execute("TRUNCATE TABLE foo.nope"));
	}

	private void createMetaTable(final String table) {
		session.execute(
			"CREATE KEYSPACE IF NOT EXISTS meta WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		session.execute(
			"CREATE TABLE IF NOT EXISTS meta.%s (id uuid PRIMARY KEY, name text)".formatted(table));
	}

	@Test
	@Order(31)
	@DisplayName("Prepared INSERT exposes variable definitions and empty result-set definitions")
	void testPreparedInsertMetadata() {
		createMetaTable("people");

		final var prepared = session.prepare("INSERT INTO meta.people (id, name) VALUES (?, ?)");
		final var variables = prepared.getVariableDefinitions();
		assertEquals(2, variables.size());
		assertEquals("id", variables.get(0).getName().asInternal());
		assertEquals(DataTypes.UUID, variables.get(0).getType());
		assertEquals("name", variables.get(1).getName().asInternal());
		assertEquals(DataTypes.TEXT, variables.get(1).getType());

		assertEquals(0, prepared.getResultSetDefinitions().size());
	}

	@Test
	@Order(32)
	@DisplayName("Prepared SELECT exposes WHERE bind markers and result-set columns")
	void testPreparedSelectMetadata() {
		createMetaTable("people");

		final var prepared = session.prepare("SELECT * FROM meta.people WHERE id = ?");
		final var variables = prepared.getVariableDefinitions();
		assertEquals(1, variables.size());
		assertEquals("id", variables.get(0).getName().asInternal());
		assertEquals(DataTypes.UUID, variables.get(0).getType());

		final var result = prepared.getResultSetDefinitions();
		assertEquals(2, result.size());
		assertTrue(result.contains("id"));
		assertTrue(result.contains("name"));
	}

	@Test
	@Order(33)
	@DisplayName("Bound statement encodes values addressable by index and by name")
	void testBoundStatementContract() {
		createMetaTable("people");

		final var prepared = session.prepare("INSERT INTO meta.people (id, name) VALUES (?, ?)");
		final var bound = prepared.bind(ANN_ID, "Ann");
		assertEquals(2, bound.size());
		assertEquals(DataTypes.UUID, bound.getType(0));
		assertEquals(0, bound.firstIndexOf("id"));
		assertEquals(1, bound.firstIndexOf("name"));
		assertEquals(ANN_ID, bound.getUuid(0));
		assertEquals("Ann", bound.getString("name"));
		assertNotNull(bound.getBytesUnsafe(0));
	}

	@Test
	@Order(34)
	@DisplayName("boundStatementBuilder produces an executable bound statement")
	void testBoundStatementBuilder() {
		createMetaTable("builders");

		final var prepared = session.prepare("INSERT INTO meta.builders (id, name) VALUES (?, ?)");
		session.execute(prepared.boundStatementBuilder(BOB_ID, "Bob").build());

		final var name = session.execute("SELECT name FROM meta.builders WHERE id = " + BOB_ID)
			.one().getString("name");
		assertEquals("Bob", name);
	}

	@Test
	@Order(35)
	@DisplayName("Binding more values than bind markers throws IllegalArgumentException")
	void testBindTooManyValues() {
		createMetaTable("people");

		final var prepared = session.prepare("INSERT INTO meta.people (id, name) VALUES (?, ?)");
		assertThrows(IllegalArgumentException.class, () -> prepared.bind(ANN_ID, "Ann", "extra"));
	}

	private void createLwtTable() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS foo.lwt (id int PRIMARY KEY, name text, age int)");
	}

	private String lwtName(final int id) {
		final var row = session.execute("SELECT name FROM foo.lwt WHERE id = " + id).one();

		return row == null ? null : row.getString("name");
	}

	@Test
	@Order(36)
	@DisplayName("INSERT IF NOT EXISTS returns [applied]=true and stores the row")
	void testInsertIfNotExistsApplied() {
		createLwtTable();

		final var resultSet = session.execute(
			"INSERT INTO foo.lwt (id, name, age) VALUES (36, 'Ada', 30) IF NOT EXISTS");

		assertTrue(resultSet.wasApplied());
		assertEquals("Ada", lwtName(36));
	}

	@Test
	@Order(37)
	@DisplayName("INSERT IF NOT EXISTS on an existing key returns [applied]=false with the current row")
	void testInsertIfNotExistsRejected() {
		createLwtTable();
		session.execute("INSERT INTO foo.lwt (id, name, age) VALUES (37, 'Grace', 40) IF NOT EXISTS");

		final var resultSet = session.execute(
			"INSERT INTO foo.lwt (id, name, age) VALUES (37, 'Hopper', 41) IF NOT EXISTS");

		assertFalse(resultSet.wasApplied());
		final var row = resultSet.one();
		assertEquals("Grace", row.getString("name"));
		assertEquals(40, row.getInt("age"));
		assertEquals("Grace", lwtName(37));
	}

	@Test
	@Order(38)
	@DisplayName("UPDATE IF <condition> applies when the condition holds")
	void testUpdateIfConditionApplied() {
		createLwtTable();
		session.execute("INSERT INTO foo.lwt (id, name, age) VALUES (38, 'old', 1)");

		final var resultSet = session.execute(
			"UPDATE foo.lwt SET name = 'new' WHERE id = 38 IF name = 'old'");

		assertTrue(resultSet.wasApplied());
		assertEquals("new", lwtName(38));
	}

	@Test
	@Order(39)
	@DisplayName("UPDATE IF <condition> is rejected and reports current values when it fails")
	void testUpdateIfConditionRejected() {
		createLwtTable();
		session.execute("INSERT INTO foo.lwt (id, name, age) VALUES (39, 'keep', 1)");

		final var resultSet = session.execute(
			"UPDATE foo.lwt SET name = 'changed' WHERE id = 39 IF name = 'wrong'");

		assertFalse(resultSet.wasApplied());
		assertEquals("keep", resultSet.one().getString("name"));
		assertEquals("keep", lwtName(39));
	}

	@Test
	@Order(40)
	@DisplayName("UPDATE IF EXISTS applies only when the row exists")
	void testUpdateIfExists() {
		createLwtTable();

		final var missing = session.execute(
			"UPDATE foo.lwt SET name = 'ghost' WHERE id = 4000 IF EXISTS");
		assertFalse(missing.wasApplied());
		assertNull(lwtName(4000));

		session.execute("INSERT INTO foo.lwt (id, name, age) VALUES (40, 'here', 1)");
		final var present = session.execute(
			"UPDATE foo.lwt SET name = 'updated' WHERE id = 40 IF EXISTS");
		assertTrue(present.wasApplied());
		assertEquals("updated", lwtName(40));
	}

	@Test
	@Order(41)
	@DisplayName("DELETE IF EXISTS applies only when the row exists")
	void testDeleteIfExists() {
		createLwtTable();

		final var missing = session.execute("DELETE FROM foo.lwt WHERE id = 4100 IF EXISTS");
		assertFalse(missing.wasApplied());

		session.execute("INSERT INTO foo.lwt (id, name, age) VALUES (41, 'doomed', 1)");
		final var present = session.execute("DELETE FROM foo.lwt WHERE id = 41 IF EXISTS");
		assertTrue(present.wasApplied());
		assertNull(lwtName(41));
	}

	@Test
	@Order(42)
	@DisplayName("DELETE IF <condition> deletes only when the condition holds")
	void testDeleteIfCondition() {
		createLwtTable();
		session.execute("INSERT INTO foo.lwt (id, name, age) VALUES (42, 'delete-me', 1)");

		final var rejected = session.execute(
			"DELETE FROM foo.lwt WHERE id = 42 IF name = 'nope'");
		assertFalse(rejected.wasApplied());
		assertEquals("delete-me", lwtName(42));

		final var applied = session.execute(
			"DELETE FROM foo.lwt WHERE id = 42 IF name = 'delete-me'");
		assertTrue(applied.wasApplied());
		assertNull(lwtName(42));
	}

	@Test
	@Order(43)
	@DisplayName("Conditional UPDATE on an undefined column throws InvalidQueryException")
	void testConditionalUpdateUndefinedColumn() {
		createLwtTable();

		assertThrows(InvalidQueryException.class, () -> session.execute(
			"UPDATE foo.lwt SET name = 'x' WHERE id = 43 IF nope = 1"));
	}

	@Test
	@Order(44)
	@DisplayName("INSERT upsert merges named columns and preserves unspecified ones")
	void testInsertUpsertMergesColumns() {
		createLwtTable();
		session.execute("INSERT INTO foo.lwt (id, name, age) VALUES (44, 'orig', 10)");

		session.execute("INSERT INTO foo.lwt (id, age) VALUES (44, 99)");

		final var row = session.execute("SELECT name, age FROM foo.lwt WHERE id = 44").one();
		assertEquals("orig", row.getString("name"));
		assertEquals(99, row.getInt("age"));
	}

	@Test
	@Order(45)
	@DisplayName("BATCH parsed from a CQL string applies every child statement")
	void testCqlStringBatch() {
		createLwtTable();

		session.execute("BEGIN BATCH "
			+ "INSERT INTO foo.lwt (id, name, age) VALUES (45, 'batch-a', 1); "
			+ "UPDATE foo.lwt SET name = 'batch-b' WHERE id = 45; "
			+ "APPLY BATCH");

		assertEquals("batch-b", lwtName(45));
	}

	@Test
	@Order(46)
	@DisplayName("Driver BatchStatement applies every child statement")
	void testDriverBatchStatement() {
		createLwtTable();

		final var batch = BatchStatement.builder(DefaultBatchType.LOGGED)
			.addStatement(
				SimpleStatement.newInstance("INSERT INTO foo.lwt (id, name) VALUES (46, 'driver-a')"))
			.addStatement(
				SimpleStatement.newInstance("UPDATE foo.lwt SET name = 'driver-b' WHERE id = 46"))
			.build();

		final var result = session.execute(batch);

		assertTrue(result.wasApplied());
		assertEquals("driver-b", lwtName(46));
	}

	@Test
	@Order(47)
	@DisplayName("A SELECT inside a batch is rejected with InvalidQueryException")
	void testSelectInBatchRejected() {
		createLwtTable();

		final var batch = BatchStatement.builder(DefaultBatchType.LOGGED)
			.addStatement(SimpleStatement.newInstance("SELECT * FROM foo.lwt WHERE id = 47"))
			.build();

		assertThrows(InvalidQueryException.class, () -> session.execute(batch));
	}

	private void createDistinctTable() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.distinct_test "
			+ "(pk int, ck int, val text, PRIMARY KEY (pk, ck))");
		session.execute("INSERT INTO foo.distinct_test (pk, ck, val) VALUES (1, 1, 'a')");
		session.execute("INSERT INTO foo.distinct_test (pk, ck, val) VALUES (1, 2, 'b')");
		session.execute("INSERT INTO foo.distinct_test (pk, ck, val) VALUES (2, 1, 'c')");
	}

	@Test
	@Order(48)
	@DisplayName("SELECT DISTINCT on the partition key returns one row per partition")
	void testSelectDistinctPartitionKey() {
		createDistinctTable();

		final var rows = session.execute("SELECT DISTINCT pk FROM foo.distinct_test").all();

		final var partitions = rows.stream().map(row -> row.getInt("pk")).collect(Collectors.toSet());
		assertEquals(Set.of(1, 2), partitions);
	}

	@Test
	@Order(49)
	@DisplayName("SELECT DISTINCT on a non-partition-key column throws InvalidQueryException")
	void testSelectDistinctNonPartitionKey() {
		createDistinctTable();

		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT DISTINCT val FROM foo.distinct_test"));
	}

	private void createIndexedTable() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.indexed (id int PRIMARY KEY, name text)");
		session.execute("CREATE INDEX IF NOT EXISTS ON foo.indexed (name)");
	}

	@Test
	@Order(50)
	@DisplayName("CREATE INDEX exposes the index through TableMetadata.getIndexes")
	void testCreateIndexExposedInMetadata() {
		createIndexedTable();

		final var indexes = session.getMetadata().getKeyspace("foo")
			.flatMap(keyspace -> keyspace.getTable("indexed"))
			.map(table -> table.getIndexes())
			.orElseThrow();
		assertTrue(indexes.containsKey(CqlIdentifier.fromInternal("indexed_name_idx")));
	}

	@Test
	@Order(51)
	@DisplayName("An indexed column can be queried without ALLOW FILTERING")
	void testQueryIndexedColumnWithoutFiltering() {
		createIndexedTable();
		session.execute("INSERT INTO foo.indexed (id, name) VALUES (1, 'indexed-a')");
		session.execute("INSERT INTO foo.indexed (id, name) VALUES (2, 'indexed-b')");

		final var rows = session.execute("SELECT id FROM foo.indexed WHERE name = 'indexed-a'").all();

		assertEquals(1, rows.size());
		assertEquals(1, rows.get(0).getInt("id"));
	}

	@Test
	@Order(52)
	@DisplayName("Indexing an undefined column throws InvalidQueryException")
	void testCreateIndexOnUndefinedColumn() {
		createIndexedTable();

		assertThrows(InvalidQueryException.class,
			() -> session.execute("CREATE INDEX ON foo.indexed (nope)"));
	}

	@Test
	@Order(53)
	@DisplayName("Creating a duplicate index throws unless IF NOT EXISTS")
	void testCreateDuplicateIndex() {
		createIndexedTable();

		assertThrows(InvalidQueryException.class,
			() -> session.execute("CREATE INDEX indexed_name_idx ON foo.indexed (name)"));
		assertDoesNotThrow(
			() -> session.execute("CREATE INDEX IF NOT EXISTS indexed_name_idx ON foo.indexed (name)"));
	}

	@Test
	@Order(54)
	@DisplayName("BoundStatement round-trips routing keyspace, idempotence, and custom payload")
	void testBoundStatementMetadata() {
		createLwtTable();
		final var prepared = session.prepare("SELECT * FROM foo.lwt WHERE id = ?");

		final var payload = Map.of("k", ByteBuffer.wrap(new byte[]{1, 2, 3}));
		final var bound = prepared.bind(1)
			.setRoutingKeyspace(CqlIdentifier.fromInternal("foo"))
			.setIdempotent(true)
			.setCustomPayload(payload);

		assertEquals(CqlIdentifier.fromInternal("foo"), bound.getRoutingKeyspace());
		assertEquals(Boolean.TRUE, bound.isIdempotent());
		assertEquals(payload, bound.getCustomPayload());
	}

	@Test
	@Order(55)
	@DisplayName("Quoted identifiers are case-sensitive; unquoted references fold to lower case")
	void testQuotedIdentifierCaseSensitivity() {
		session.execute("CREATE TABLE foo.quoting (id int PRIMARY KEY, \"MixedCase\" text)");
		session.execute("INSERT INTO foo.quoting (id, \"MixedCase\") VALUES (1, 'v')");

		final var row = session.execute("SELECT \"MixedCase\" FROM foo.quoting WHERE id = 1").one();
		assertEquals("v", row.getString(0));

		// An unquoted reference folds to lower case ("mixedcase"), which is not a defined column.
		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT mixedcase FROM foo.quoting WHERE id = 1"));
	}

	@Test
	@Order(56)
	@DisplayName("A bound UdtValue round-trips through INSERT and SELECT")
	void testUdtValueRoundTrip() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.addr (street text, city text)");
		session.execute(
			"CREATE TABLE IF NOT EXISTS foo.udt_people (id uuid PRIMARY KEY, home frozen<addr>)");

		final var addr = session.getMetadata().getKeyspace("foo").orElseThrow()
			.getUserDefinedType("addr").orElseThrow();
		final var home = addr.newValue().setString("street", "Main").setString("city", "Anytown");

		final var prepared = session.prepare(
			"INSERT INTO foo.udt_people (id, home) VALUES (?, ?)");
		session.execute(prepared.bind(ANN_ID, home));

		final var row = session.execute(
			"SELECT home FROM foo.udt_people WHERE id = " + ANN_ID).one();
		assertNotNull(row);

		final var readBack = row.getUdtValue("home");
		assertNotNull(readBack);
		assertEquals("Main", readBack.getString("street"));
		assertEquals("Anytown", readBack.getString("city"));
	}

	private void createCompositeKeyTable() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.composite "
			+ "(pk1 int, pk2 int, cc int, v int, PRIMARY KEY ((pk1, pk2), cc))");
	}

	@Test
	@Order(57)
	@DisplayName("Partition key indices are ordered by partition key position, not bind order")
	void testPartitionKeyIndicesOrderedByPosition() {
		createCompositeKeyTable();

		final var inOrder = session.prepare(
			"SELECT v FROM foo.composite WHERE pk1 = ? AND pk2 = ? AND cc = ?");
		assertEquals(List.of(0, 1), inOrder.getPartitionKeyIndices());

		// pk2 is bound first, but pk1 is the first partition key component.
		final var reversed = session.prepare(
			"SELECT v FROM foo.composite WHERE pk2 = ? AND pk1 = ? AND cc = ?");
		assertEquals(List.of(1, 0), reversed.getPartitionKeyIndices());
	}

	@Test
	@Order(58)
	@DisplayName("Partition key indices are empty unless every component is a bind marker")
	void testPartitionKeyIndicesRequireEveryComponent() {
		createCompositeKeyTable();

		final var hardCoded = session.prepare(
			"UPDATE foo.composite SET v = ? WHERE pk1 = 1 AND pk2 = ? AND cc = ?");
		assertEquals(List.of(), hardCoded.getPartitionKeyIndices());

		final var bound = session.prepare(
			"UPDATE foo.composite SET v = ? WHERE pk1 = ? AND pk2 = ? AND cc = ?");
		assertEquals(List.of(1, 2), bound.getPartitionKeyIndices());
	}

	@Test
	@Order(59)
	@DisplayName("INSERT exposes partition key indices from the VALUES clause")
	void testPartitionKeyIndicesForInsert() {
		createCompositeKeyTable();

		final var prepared = session.prepare(
			"INSERT INTO foo.composite (cc, pk2, v, pk1) VALUES (?, ?, ?, ?)");

		assertEquals(List.of(3, 1), prepared.getPartitionKeyIndices());
	}

	@Test
	@Order(60)
	@DisplayName("A statement with no partition key markers has no partition key indices")
	void testPartitionKeyIndicesWithoutMarkers() {
		createCompositeKeyTable();

		final var prepared = session.prepare("SELECT v FROM foo.composite WHERE pk1 = 1 AND pk2 = 2");

		assertEquals(List.of(), prepared.getPartitionKeyIndices());
	}

	@Test
	@Order(61)
	@DisplayName("A LIMIT marker does not shift partition key indices")
	void testPartitionKeyIndicesIgnoreLimitMarker() {
		createCompositeKeyTable();

		final var prepared = session.prepare(
			"SELECT v FROM foo.composite WHERE pk1 = ? AND pk2 = ? LIMIT ?");

		assertEquals(List.of(0, 1), prepared.getPartitionKeyIndices());
	}

	@Test
	@Order(62)
	@DisplayName("The routing key of a single-component partition key is the encoded value")
	void testRoutingKeySingleComponent() {
		createLwtTable();

		final var bound = session.prepare("SELECT name FROM foo.lwt WHERE id = ?").bind(7);

		assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0, 7}), bound.getRoutingKey());
	}

	@Test
	@Order(63)
	@DisplayName("A composite routing key is length-prefixed in partition key order")
	void testRoutingKeyCompositeComponents() {
		createCompositeKeyTable();

		final var bound = session.prepare(
			"SELECT v FROM foo.composite WHERE pk2 = ? AND pk1 = ? AND cc = ?").bind(9, 7, 1);

		// Two-byte length, value, then a zero byte per component; pk1 precedes pk2 despite bind order.
		final var expected = ByteBuffer.wrap(new byte[]{0, 4, 0, 0, 0, 7, 0, 0, 4, 0, 0, 0, 9, 0});
		assertEquals(expected, bound.getRoutingKey());
	}

	@Test
	@Order(64)
	@DisplayName("An explicitly set routing key overrides the partition key")
	void testRoutingKeyExplicitOverride() {
		createLwtTable();

		final var override = ByteBuffer.wrap(new byte[]{1, 2, 3});
		final var bound = session.prepare("SELECT name FROM foo.lwt WHERE id = ?")
			.bind(7)
			.setRoutingKey(override);

		assertEquals(override, bound.getRoutingKey());
	}

	@Test
	@Order(65)
	@DisplayName("The routing key is null when a partition key component is not a bind marker")
	void testRoutingKeyWithoutMarkers() {
		createCompositeKeyTable();

		final var bound = session.prepare(
			"SELECT v FROM foo.composite WHERE pk1 = 1 AND pk2 = ? AND cc = ?").bind(9, 1);

		assertNull(bound.getRoutingKey());
	}

	@Test
	@Order(66)
	@DisplayName("The routing key is null when a partition key value is unset")
	void testRoutingKeyWithUnsetValue() {
		createCompositeKeyTable();

		final var bound = session.prepare(
			"SELECT v FROM foo.composite WHERE pk1 = ? AND pk2 = ? AND cc = ?").bind();

		assertNull(bound.getRoutingKey());
	}

	@Test
	@Order(67)
	@DisplayName("A rethrown DriverException carries execution info")
	void testDriverExceptionCarriesExecutionInfo() {
		createLwtTable();

		final var error = assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT nope FROM foo.lwt"));

		final var executionInfo = error.getExecutionInfo();
		assertNotNull(executionInfo);
		assertEquals("SELECT nope FROM foo.lwt",
			((SimpleStatement) executionInfo.getRequest()).getQuery());
		assertTrue(executionInfo.getWarnings().isEmpty());
		assertTrue(executionInfo.isSchemaInAgreement());
	}

	private UserDefinedType userDefinedType(final String name) {
		return session.getMetadata()
			.getKeyspace("foo")
			.orElseThrow()
			.getUserDefinedType(name)
			.orElseThrow();
	}

	@Test
	@Order(68)
	@DisplayName("ALTER TYPE ADD appends a field, leaving it null on existing values")
	void testAlterTypeAddField() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.alter_add (street text)");
		session.execute("CREATE TABLE IF NOT EXISTS foo.alter_add_users "
			+ "(id int PRIMARY KEY, home frozen<alter_add>)");
		// A UDT literal in an INSERT is not supported by SeaStar yet, so bind the value instead.
		final var existing = userDefinedType("alter_add").newValue().setString("street", "Main");
		session.execute(
			session.prepare("INSERT INTO foo.alter_add_users (id, home) VALUES (?, ?)")
				.bind(1, existing));

		session.execute("ALTER TYPE foo.alter_add ADD zip int");

		final var type = userDefinedType("alter_add");
		assertEquals(List.of(CqlIdentifier.fromInternal("street"), CqlIdentifier.fromInternal("zip")),
			type.getFieldNames());
		assertEquals(List.of(DataTypes.TEXT, DataTypes.INT), type.getFieldTypes());

		final var home = session.execute("SELECT home FROM foo.alter_add_users WHERE id = 1")
			.one()
			.getUdtValue("home");
		assertEquals("Main", home.getString("street"));
		assertTrue(home.isNull("zip"));
	}

	@Test
	@Order(69)
	@DisplayName("ALTER TYPE ADD rejects a duplicate field unless IF NOT EXISTS")
	void testAlterTypeAddDuplicateField() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.alter_dup (street text)");

		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE foo.alter_dup ADD street text"));
		assertDoesNotThrow(
			() -> session.execute("ALTER TYPE foo.alter_dup ADD IF NOT EXISTS street text"));

		assertEquals(List.of(CqlIdentifier.fromInternal("street")),
			userDefinedType("alter_dup").getFieldNames());
	}

	@Test
	@Order(70)
	@DisplayName("Altering an undefined type throws unless IF EXISTS")
	void testAlterTypeUndefined() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE foo.nope ADD zip int"));
		assertDoesNotThrow(() -> session.execute("ALTER TYPE IF EXISTS foo.nope ADD zip int"));
	}

	@Test
	@Order(71)
	@DisplayName("ALTER TYPE RENAME renames fields in place")
	void testAlterTypeRenameFields() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.alter_rename (street text, city text)");

		session.execute("ALTER TYPE foo.alter_rename RENAME street TO road AND city TO town");

		final var type = userDefinedType("alter_rename");
		assertEquals(List.of(CqlIdentifier.fromInternal("road"), CqlIdentifier.fromInternal("town")),
			type.getFieldNames());
		assertEquals(List.of(DataTypes.TEXT, DataTypes.TEXT), type.getFieldTypes());
	}

	@Test
	@Order(72)
	@DisplayName("ALTER TYPE RENAME rejects unknown and duplicate field names")
	void testAlterTypeRenameInvalidFields() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.alter_rename_bad (street text, city text)");

		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE foo.alter_rename_bad RENAME nope TO road"));
		assertDoesNotThrow(
			() -> session.execute("ALTER TYPE foo.alter_rename_bad RENAME IF EXISTS nope TO road"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE foo.alter_rename_bad RENAME street TO city"));

		assertEquals(List.of(CqlIdentifier.fromInternal("street"), CqlIdentifier.fromInternal("city")),
			userDefinedType("alter_rename_bad").getFieldNames());
	}

	@Test
	@Order(73)
	@DisplayName("Altering the type of a field is no longer supported")
	void testAlterTypeAlterFieldUnsupported() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.alter_field (street text)");

		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE foo.alter_field ALTER street TYPE int"));
	}

	private void createUdtLiteralTable() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.lit_addr (street text, zip int)");
		session.execute("CREATE TABLE IF NOT EXISTS foo.lit_people "
			+ "(id int PRIMARY KEY, home frozen<lit_addr>)");
	}

	@Test
	@Order(74)
	@DisplayName("A UDT literal in an INSERT round-trips")
	void testInsertUdtLiteral() {
		createUdtLiteralTable();

		session.execute(
			"INSERT INTO foo.lit_people (id, home) VALUES (1, {street: 'Main', zip: 12345})");

		final var home = session.execute("SELECT home FROM foo.lit_people WHERE id = 1")
			.one()
			.getUdtValue("home");
		assertEquals("Main", home.getString("street"));
		assertEquals(12345, home.getInt("zip"));
	}

	@Test
	@Order(75)
	@DisplayName("Fields omitted from a UDT literal read back null")
	void testInsertPartialUdtLiteral() {
		createUdtLiteralTable();

		session.execute("INSERT INTO foo.lit_people (id, home) VALUES (2, {street: 'Elm'})");

		final var home = session.execute("SELECT home FROM foo.lit_people WHERE id = 2")
			.one()
			.getUdtValue("home");
		assertEquals("Elm", home.getString("street"));
		assertTrue(home.isNull("zip"));
	}

	@Test
	@Order(76)
	@DisplayName("An unknown field in a UDT literal throws InvalidQueryException")
	void testInsertUdtLiteralUnknownField() {
		createUdtLiteralTable();

		assertThrows(InvalidQueryException.class, () -> session.execute(
			"INSERT INTO foo.lit_people (id, home) VALUES (3, {street: 'Oak', nope: 1})"));
	}

	@Test
	@Order(77)
	@DisplayName("A UDT literal nests other UDT literals and bind markers")
	void testInsertNestedUdtLiteral() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.lit_inner (city text)");
		session.execute(
			"CREATE TYPE IF NOT EXISTS foo.lit_outer (street text, place frozen<lit_inner>)");
		session.execute("CREATE TABLE IF NOT EXISTS foo.lit_nested "
			+ "(id int PRIMARY KEY, home frozen<lit_outer>)");

		final var prepared = session.prepare(
			"INSERT INTO foo.lit_nested (id, home) VALUES (?, {street: ?, place: {city: 'Anytown'}})");

		// A marker inside a UDT literal is typed as the field it stands in for, and named column.field.
		final var variables = prepared.getVariableDefinitions();
		assertEquals(2, variables.size());
		assertEquals(DataTypes.INT, variables.get(0).getType());
		assertEquals(DataTypes.TEXT, variables.get(1).getType());
		assertEquals("home.street", variables.get(1).getName().asInternal());

		session.execute(prepared.bind(1, "Main"));

		final var home = session.execute("SELECT home FROM foo.lit_nested WHERE id = 1")
			.one()
			.getUdtValue("home");
		assertEquals("Main", home.getString("street"));
		assertEquals("Anytown", home.getUdtValue("place").getString("city"));
	}

	@Test
	@Order(78)
	@DisplayName("A UDT literal can be assigned by UPDATE and compared by an IF condition")
	void testUpdateUdtLiteral() {
		createUdtLiteralTable();
		session.execute("INSERT INTO foo.lit_people (id, home) VALUES (10, {street: 'Oak', zip: 9})");

		session.execute("UPDATE foo.lit_people SET home = {street: 'Pine', zip: 7} WHERE id = 10");
		assertEquals("Pine", session.execute("SELECT home FROM foo.lit_people WHERE id = 10")
			.one()
			.getUdtValue("home")
			.getString("street"));

		final var rejected = session.execute("UPDATE foo.lit_people SET home = {street: 'Elm'} "
			+ "WHERE id = 10 IF home = {street: 'Oak', zip: 9}");
		assertFalse(rejected.wasApplied());

		final var applied = session.execute("UPDATE foo.lit_people SET home = {street: 'Elm'} "
			+ "WHERE id = 10 IF home = {street: 'Pine', zip: 7}");
		assertTrue(applied.wasApplied());
	}

	@Test
	@Order(79)
	@DisplayName("A UDT literal can select and delete by a UDT partition key")
	void testSelectAndDeleteByUdtLiteral() {
		session.execute("CREATE TYPE IF NOT EXISTS foo.lit_addr (street text, zip int)");
		session.execute("CREATE TABLE IF NOT EXISTS foo.lit_keyed "
			+ "(home frozen<lit_addr> PRIMARY KEY, note text)");
		session.execute(
			"INSERT INTO foo.lit_keyed (home, note) VALUES ({street: 'Main', zip: 1}, 'a')");

		final var row = session.execute(
			"SELECT note FROM foo.lit_keyed WHERE home = {street: 'Main', zip: 1}").one();
		assertNotNull(row);
		assertEquals("a", row.getString("note"));

		session.execute("DELETE FROM foo.lit_keyed WHERE home = {street: 'Main', zip: 1}");
		assertNull(session.execute(
			"SELECT note FROM foo.lit_keyed WHERE home = {street: 'Main', zip: 1}").one());
	}

	@Test
	@Order(80)
	@DisplayName("A quoted column keeps its case while an unquoted one folds to lower case")
	void testCaseSensitiveColumnNames() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.cased "
			+ "(id int PRIMARY KEY, \"myColumn\" int, MixedCase int)");
		session.execute("INSERT INTO foo.cased (id, \"myColumn\", MixedCase) VALUES (1, 2, 3)");

		// An unquoted identifier folds to lower case wherever it appears, select clause included.
		final var folded = session.execute("SELECT MixedCase FROM foo.cased WHERE ID = 1").one();
		assertNotNull(folded);
		assertEquals(3, folded.getInt("mixedcase"));

		// A quoted identifier is matched exactly, so the two spellings are different columns.
		final var quoted = session.execute("SELECT \"myColumn\" FROM foo.cased WHERE id = 1").one();
		assertNotNull(quoted);
		assertEquals(2, quoted.getInt("\"myColumn\""));

		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT myColumn FROM foo.cased WHERE id = 1"));
	}

	@Test
	@Order(81)
	@DisplayName("An unqualified statement resolves its table against the session keyspace")
	void testUnqualifiedStatementsUseSessionKeyspace() {
		session.execute("USE foo");
		session.execute("CREATE TABLE IF NOT EXISTS foo.unqualified (id int PRIMARY KEY, name text)");

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
		session.execute("USE foo");
		session.execute(
			"CREATE TABLE IF NOT EXISTS foo.unqualified_prepared (id int PRIMARY KEY, name text)");

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
		session.execute("CREATE TABLE IF NOT EXISTS foo.exec_info (id int PRIMARY KEY)");

		final var executionInfo = session.execute("SELECT * FROM foo.exec_info")
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
	@Order(86)
	@DisplayName("Keyspace metadata reports the replication and durable writes it was created with")
	void testKeyspaceReplicationMetadata() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS repl_default WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		session.execute("CREATE KEYSPACE IF NOT EXISTS repl_durable_off WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 } AND durable_writes = false");

		final var defaults = keyspaceMetadata("repl_default");
		assertEquals(
			Map.of("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor",
				"1"), defaults.getReplication());
		assertTrue(defaults.isDurableWrites());

		final var durableOff = keyspaceMetadata("repl_durable_off");
		assertEquals("2", durableOff.getReplication().get("replication_factor"));
		assertFalse(durableOff.isDurableWrites());
	}

	private KeyspaceMetadata keyspaceMetadata(final String name) {
		return session.getMetadata()
			.getKeyspace(name)
			.orElseThrow(() -> new IllegalStateException(
				"keyspace %s is required to read its metadata".formatted(name)));
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
	}

	@Test
	@Order(88)
	@DisplayName("Collection, tuple and vector literals insert and read back as their Java values")
	void testCollectionLiterals() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.literals (id int PRIMARY KEY, l list<int>, "
			+ "s set<int>, m map<text, int>, t tuple<int, text>, v vector<float, 2>)");
		session.execute("INSERT INTO foo.literals (id, l, s, m, t, v) VALUES "
			+ "(1, [1, 2], {4, 3}, {'a': 5}, (6, 'x'), [1.5, 2.5])");

		final var row = session.execute("SELECT * FROM foo.literals WHERE id = 1").one();
		assertNotNull(row);
		assertEquals(List.of(1, 2), row.getList("l", Integer.class));
		assertEquals(Set.of(3, 4), row.getSet("s", Integer.class));
		assertEquals(Map.of("a", 5), row.getMap("m", String.class, Integer.class));

		final var tuple = row.getTupleValue("t");
		assertNotNull(tuple);
		assertEquals(6, tuple.getInt(0));
		assertEquals("x", tuple.getString(1));

		final var vector = row.getVector("v", Float.class);
		assertNotNull(vector);
		assertEquals(List.of(1.5f, 2.5f), vector.stream().toList());
	}

	@Test
	@Order(89)
	@DisplayName("A literal the column's type cannot take is rejected, {} included")
	void testCollectionLiteralTypeErrors() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.literal_errors "
			+ "(id int PRIMARY KEY, l list<int>, s set<int>, m map<text, int>)");

		// {} is parsed as an empty set because the grammar cannot tell it from an empty map, so a
		// list column rejects it, and [] - which is a list or a vector - is rejected by a set.
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO foo.literal_errors (id, l) VALUES (1, {})"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO foo.literal_errors (id, s) VALUES (1, [])"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO foo.literal_errors (id, s) VALUES (1, {'a': 1})"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO foo.literal_errors (id, m) VALUES (1, {1: 1})"));
	}

	@Test
	@Order(90)
	@DisplayName("An empty collection is null unless it is frozen, and its getter still answers empty")
	void testEmptyCollectionLiterals() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.empties (id int PRIMARY KEY, l list<int>, "
			+ "s set<int>, m map<text, int>, fl frozen<list<int>>, fs frozen<set<int>>, "
			+ "fm frozen<map<text, int>>)");
		session.execute(
			"INSERT INTO foo.empties (id, l, s, m, fl, fs, fm) VALUES (1, [], {}, {}, [], {}, {})");

		final var row = session.execute("SELECT * FROM foo.empties WHERE id = 1").one();
		assertNotNull(row);

		// An unfrozen collection is one cell per element, so an empty one is no cells at all.
		assertTrue(row.isNull("l"));
		assertTrue(row.isNull("s"));
		assertTrue(row.isNull("m"));
		// A frozen collection is a single value, and an empty one is still a value.
		assertFalse(row.isNull("fl"));
		assertFalse(row.isNull("fs"));
		assertFalse(row.isNull("fm"));

		assertEquals(List.of(), row.getList("l", Integer.class));
		assertEquals(Set.of(), row.getSet("s", Integer.class));
		assertEquals(Map.of(), row.getMap("m", String.class, Integer.class));
		assertEquals(List.of(), row.getList("fl", Integer.class));
		assertEquals(Set.of(), row.getSet("fs", Integer.class));
		assertEquals(Map.of(), row.getMap("fm", String.class, Integer.class));
	}

	@Test
	@Order(91)
	@DisplayName("A null literal clears the column it is written to")
	void testNullLiteral() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS foo.null_literals (id int PRIMARY KEY, name text, tags list<int>)");
		session.execute("INSERT INTO foo.null_literals (id, name, tags) VALUES (1, 'Ann', [1])");
		session.execute("INSERT INTO foo.null_literals (id, name, tags) VALUES (1, null, null)");

		final var row = session.execute("SELECT * FROM foo.null_literals WHERE id = 1").one();
		assertNotNull(row);
		assertNull(row.getString("name"));
		assertTrue(row.isNull("tags"));
	}

	@Test
	@Order(92)
	@DisplayName("now(), uuid() and currentTimestamp() are evaluated when the statement runs")
	void testTermFunctions() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.functions "
			+ "(id int PRIMARY KEY, tu timeuuid, u uuid, ts timestamp)");
		final var before = Instant.now().minusSeconds(60);
		session.execute("INSERT INTO foo.functions (id, tu, u, ts) VALUES "
			+ "(1, now(), uuid(), currentTimestamp())");

		final var row = session.execute("SELECT * FROM foo.functions WHERE id = 1").one();
		assertNotNull(row);
		assertEquals(1, row.getUuid("tu").version());
		assertEquals(4, row.getUuid("u").version());
		assertTrue(row.getInstant("ts").isAfter(before));

		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO foo.functions (id, u) VALUES (2, wat())"));
		// A function whose result the column cannot hold is a type error, not an unknown function.
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO foo.functions (id, tu) VALUES (2, currentTimestamp())"));
	}

	@Test
	@Order(93)
	@DisplayName("A type cast is accepted for the column's own type and rejected for any other")
	void testTypeCast() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.casts (id int PRIMARY KEY, name text)");
		session.execute("INSERT INTO foo.casts (id, name) VALUES ((int) 1, (text) 'Ann')");

		final var row = session.execute("SELECT name FROM foo.casts WHERE id = 1").one();
		assertNotNull(row);
		assertEquals("Ann", row.getString("name"));

		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO foo.casts (id, name) VALUES ((bigint) 2, 'Bob')"));
	}

	@Test
	@Order(94)
	@DisplayName("Binding a value its column cannot hold throws CodecNotFoundException at bind time")
	void testBoundValueTypeChecking() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.bound "
			+ "(id int PRIMARY KEY, name text, tags list<int>)");
		final var prepared = session.prepare(
			"INSERT INTO foo.bound (id, name, tags) VALUES (?, ?, ?)");

		assertThrows(CodecNotFoundException.class, () -> prepared.bind(1, 2, List.of(3)));
		assertThrows(CodecNotFoundException.class, () -> prepared.bind(1L, "Ann", List.of(3)));
		assertThrows(CodecNotFoundException.class, () -> prepared.bind(1, "Ann", List.of("x")));

		assertDoesNotThrow(() -> session.execute(prepared.bind(1, "Ann", List.of(3))));
		// A null binds to any column, and trailing markers may be left unbound.
		assertDoesNotThrow(() -> session.execute(prepared.bind(2, null, null)));
		assertDoesNotThrow(() -> session.execute(prepared.bind(3)));

		final var row = session.execute("SELECT * FROM foo.bound WHERE id = 1").one();
		assertNotNull(row);
		assertEquals("Ann", row.getString("name"));
		assertEquals(List.of(3), row.getList("tags", Integer.class));
	}

	@Test
	@Order(95)
	@DisplayName("executeAsync reports every failure as a failed stage, never as a throw")
	void testExecuteAsyncNeverThrows() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.async_fail (id int PRIMARY KEY, name text)");

		final var queries = List.of("SELECT * FROM no_such_keyspace.t",
			"SELECT * FROM foo.no_such_table", "SELECT no_such_col FROM foo.async_fail",
			"INSERT INTO no_such_keyspace.t (id) VALUES (1)", "SELECT FROM WHERE",
			"CREATE MATERIALIZED VIEW foo.mv AS SELECT * FROM foo.async_fail "
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
	@Order(96)
	@DisplayName("prepare rejects a statement naming a keyspace, table or column that does not exist")
	void testPrepareValidatesAgainstTheSchema() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.prep (id int PRIMARY KEY, name text)");

		assertThrows(InvalidQueryException.class,
			() -> session.prepare("SELECT * FROM foo.no_such_table WHERE id = ?"));
		assertThrows(InvalidQueryException.class,
			() -> session.prepare("SELECT * FROM no_such_keyspace.prep WHERE id = ?"));
		assertThrows(InvalidQueryException.class,
			() -> session.prepare("INSERT INTO foo.prep (id, no_such_col) VALUES (?, ?)"));
		assertThrows(InvalidQueryException.class,
			() -> session.prepare("SELECT no_such_col FROM foo.prep WHERE id = ?"));
		assertThrows(InvalidQueryException.class,
			() -> session.prepare("UPDATE foo.prep SET no_such_col = ? WHERE id = ?"));
		assertThrows(SyntaxError.class, () -> session.prepare("SELECT FROM WHERE"));

		// A statement that addresses no table carries no markers, and preparing it succeeds.
		assertDoesNotThrow(() -> session.prepare("TRUNCATE foo.prep"));
		assertDoesNotThrow(() -> session.prepare("SELECT * FROM foo.prep WHERE id = ?"));
	}

	@Test
	@Order(97)
	@DisplayName("getResultMetadataId returns a readable, stable, read-only identifier")
	void testResultMetadataIdIsReadable() {
		session.execute("CREATE TABLE IF NOT EXISTS foo.meta_id (id int PRIMARY KEY, name text)");
		final var prepared = session.prepare("SELECT * FROM foo.meta_id WHERE id = ?");

		final var id = prepared.getResultMetadataId();
		assertTrue(id.remaining() > 0, "the identifier must not be an empty buffer");
		assertTrue(id.isReadOnly());

		// The id is opaque but stable: asking twice describes the same result metadata.
		assertEquals(id, prepared.getResultMetadataId());
	}

	@AfterAll
	void afterAll() {
		session.close();
	}

}
