package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import java.util.List;
import java.util.UUID;
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
	void testSimpleSelect() {
		// This test populates data directly via the SeaStar model, so it only runs against SeaStar.
		assumeTrue(session.getContext() instanceof VolatileDriverContext);
		final var context = (VolatileDriverContext) session.getContext();
		final var keyspace = context.newSeaStarKeyspace("foo");
		final var tableName = CqlIdentifier.fromInternal("bar");
		final var table = keyspace.newSeaStarTable(tableName);
		table.addColumn("foo", DataTypes.TEXT);
		table.addColumn("bar", DataTypes.TEXT);
		table.addRow("foo", "bar");

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

	@AfterAll
	void afterAll() {
		session.close();
	}

}
