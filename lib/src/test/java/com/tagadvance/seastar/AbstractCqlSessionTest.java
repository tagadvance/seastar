package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
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

// TODO: all tests should receive an instance of CqlSession with the container session and seastar session being supplied, perhaps by nesting
// TODO: test with and without quotes
// TODO: user defined types;
// TODO: simulate filtering failure
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
abstract class AbstractCqlSessionTest {

	protected abstract CqlSession createInstance();

	private static CqlSession session;

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
		// TODO: TABLE BUILDER / populate from query
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
	}

	private static final UUID ANN_ID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
	private static final UUID BOB_ID = UUID.fromString("223e4567-e89b-12d3-a456-426614174001");
	private static final UUID CAROL_ID = UUID.fromString("323e4567-e89b-12d3-a456-426614174002");

	private static String nameOf(final UUID id) {
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

	@AfterAll
	static void afterAll() {
		session.close();
	}

}
