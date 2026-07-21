package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.jupiter.api.Test;

// TODO: all tests should receive an instance of CqlSession with the container session and seastar session being supplied, perhaps by nesting
// TODO: test with and without quotes
// TODO: user defined types;
// TODO: simulate filtering failure
class SeaStarCqlSessionTest {

	@Test
	void testSimpleSelect() {
		try (final var session = SeaStarCqlSession.builder().build()) {
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
	}

	@Test
	void testCreateKeyspace() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			assertTrue(context.getSeaStarKeyspace(CqlIdentifier.fromInternal("foo")).isEmpty());

			final var resultSet1 = session.execute(
				"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			assertNotNull(resultSet1);

			final var resultSet2 = assertDoesNotThrow(() -> session.execute(
				"CREATE KEYSPACE IF NOT EXISTS foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));
			assertNotNull(resultSet2);

			assertThrows(AlreadyExistsException.class, () -> session.execute(
				"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));

			assertTrue(context.getSeaStarKeyspace(CqlIdentifier.fromInternal("foo")).isPresent());
		}
	}

	@Test
	void testUseKeyspace() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			assertTrue(context.getSeaStarKeyspace(CqlIdentifier.fromInternal("foo")).isEmpty());

			final var resultSet1 = session.execute(
				"CREATE KEYSPACE \"foo\" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			assertNotNull(resultSet1);

			assertDoesNotThrow(() -> session.execute("USE \"foo\";"));

			final var keyspace = session.getKeyspace();
			assertTrue(keyspace.isPresent());
			assertEquals("foo", keyspace.get().asInternal());
		}
	}

	@Test
	void testCreateSimpleTable() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			session.execute(
				"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			session.execute("USE foo");

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

			assertTrue(context.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarTable("users"))
				.isPresent());
		}
	}

	@Test
	void testCreateTableWithPrimaryKeyAndClusteringColumn() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			session.execute(
				"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			session.execute("USE foo");

			final var resultSet1 = session.execute("""
				CREATE TABLE user_status_updates (
					user_id uuid,
					updated_at timestamp,
					status text,
					PRIMARY KEY (user_id, updated_at)
				);""");
			assertNotNull(resultSet1);

			assertTrue(context.getSeaStarKeyspace("user_status_updates")
				.flatMap(keyspace -> keyspace.getSeaStarTable("users"))
				.isPresent());
		}
	}

	@Test
	void testCreateTableWithPrimaryKeysAndClusteringColumn() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			session.execute(
				"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			session.execute("USE foo");

			final var resultSet1 = session.execute("""
				CREATE TABLE device_metrics (
					device_id uuid,
					log_date date,
					log_time time,
					metric_value double,
					PRIMARY KEY ((device_id, log_date), log_time)
				);""");
			assertNotNull(resultSet1);

			assertTrue(context.getSeaStarKeyspace("device_metrics")
				.flatMap(keyspace -> keyspace.getSeaStarTable("users"))
				.isPresent());
		}
	}

	@Test
	void testNewTableWithAllPossibleDataTypes() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			session.execute(
				"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			session.execute("USE foo");

			final var resultSet1 = session.execute("""
				CREATE TYPE IF NOT EXISTS "foo".phone_profile (
					country_code int,
					phone_number text
				);""");
			assertNotNull(resultSet1);

			assertTrue(context.getSeaStarKeyspace("foo")
				.flatMap(keyspace -> keyspace.getSeaStarUserDefinedType("phone_profile"))
				.isPresent());

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

	}


}
