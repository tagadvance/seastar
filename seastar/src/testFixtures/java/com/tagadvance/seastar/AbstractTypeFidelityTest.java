package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * User defined types: every CQL column type a table can carry, the UDT literal forms, ALTER TYPE
 * and DROP TYPE. This group owns the {@code types} keyspace.
 */
public abstract class AbstractTypeFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("types");
	}

	@Test
	@Order(7)
	@DisplayName("CREATE TABLE accepts every CQL column type, collections, tuples, UDTs and vectors included")
	void testNewTableWithAllPossibleDataTypes() {
		final var context = session.getContext();

		final var resultSet1 = session.execute("""
			CREATE TYPE IF NOT EXISTS "types".phone_profile (
				country_code int,
				phone_number text
			);""");
		assertNotNull(resultSet1);

		if (context instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(seaStarContext.getSeaStarKeyspace("types")
				.flatMap(keyspace -> keyspace.getSeaStarUserDefinedType("phone_profile"))
				.isPresent());
		}

		final var resultSet2 = session.execute("""
			CREATE TABLE IF NOT EXISTS types.all_types_master (
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
			final var table = seaStarContext.getSeaStarKeyspace("types")
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
			"CREATE TABLE types.undefined_udt_ref (id uuid PRIMARY KEY, profile frozen<no_such_type>)"));
	}

	private static final UUID ID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

	@Test
	@Order(56)
	@DisplayName("A bound UdtValue round-trips through INSERT and SELECT")
	void testUdtValueRoundTrip() {
		session.execute("CREATE TYPE IF NOT EXISTS types.addr (street text, city text)");
		session.execute(
			"CREATE TABLE IF NOT EXISTS types.udt_people (id uuid PRIMARY KEY, home frozen<addr>)");

		final var addr = session.getMetadata().getKeyspace("types").orElseThrow()
			.getUserDefinedType("addr").orElseThrow();
		final var home = addr.newValue().setString("street", "Main").setString("city", "Anytown");

		final var prepared = session.prepare(
			"INSERT INTO types.udt_people (id, home) VALUES (?, ?)");
		session.execute(prepared.bind(ID, home));

		final var row = session.execute(
			"SELECT home FROM types.udt_people WHERE id = " + ID).one();
		assertNotNull(row);

		final var readBack = row.getUdtValue("home");
		assertNotNull(readBack);
		assertEquals("Main", readBack.getString("street"));
		assertEquals("Anytown", readBack.getString("city"));
	}

	private UserDefinedType userDefinedType(final String name) {
		return session.getMetadata()
			.getKeyspace("types")
			.orElseThrow()
			.getUserDefinedType(name)
			.orElseThrow();
	}

	@Test
	@Order(68)
	@DisplayName("ALTER TYPE ADD appends a field, leaving it null on existing values")
	void testAlterTypeAddField() {
		session.execute("CREATE TYPE IF NOT EXISTS types.alter_add (street text)");
		session.execute("CREATE TABLE IF NOT EXISTS types.alter_add_users "
			+ "(id int PRIMARY KEY, home frozen<alter_add>)");
		// A UDT literal in an INSERT is not supported by SeaStar yet, so bind the value instead.
		final var existing = userDefinedType("alter_add").newValue().setString("street", "Main");
		session.execute(
			session.prepare("INSERT INTO types.alter_add_users (id, home) VALUES (?, ?)")
				.bind(1, existing));

		session.execute("ALTER TYPE types.alter_add ADD zip int");

		final var type = userDefinedType("alter_add");
		assertEquals(List.of(CqlIdentifier.fromInternal("street"), CqlIdentifier.fromInternal("zip")),
			type.getFieldNames());
		assertEquals(List.of(DataTypes.TEXT, DataTypes.INT), type.getFieldTypes());

		final var home = session.execute("SELECT home FROM types.alter_add_users WHERE id = 1")
			.one()
			.getUdtValue("home");
		assertEquals("Main", home.getString("street"));
		assertTrue(home.isNull("zip"));
	}

	@Test
	@Order(69)
	@DisplayName("ALTER TYPE ADD rejects a duplicate field unless IF NOT EXISTS")
	void testAlterTypeAddDuplicateField() {
		session.execute("CREATE TYPE IF NOT EXISTS types.alter_dup (street text)");

		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE types.alter_dup ADD street text"));
		assertDoesNotThrow(
			() -> session.execute("ALTER TYPE types.alter_dup ADD IF NOT EXISTS street text"));

		assertEquals(List.of(CqlIdentifier.fromInternal("street")),
			userDefinedType("alter_dup").getFieldNames());
	}

	@Test
	@Order(70)
	@DisplayName("Altering an undefined type throws unless IF EXISTS")
	void testAlterTypeUndefined() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE types.nope ADD zip int"));
		assertDoesNotThrow(() -> session.execute("ALTER TYPE IF EXISTS types.nope ADD zip int"));
	}

	@Test
	@Order(71)
	@DisplayName("ALTER TYPE RENAME renames fields in place")
	void testAlterTypeRenameFields() {
		session.execute("CREATE TYPE IF NOT EXISTS types.alter_rename (street text, city text)");

		session.execute("ALTER TYPE types.alter_rename RENAME street TO road AND city TO town");

		final var type = userDefinedType("alter_rename");
		assertEquals(List.of(CqlIdentifier.fromInternal("road"), CqlIdentifier.fromInternal("town")),
			type.getFieldNames());
		assertEquals(List.of(DataTypes.TEXT, DataTypes.TEXT), type.getFieldTypes());
	}

	@Test
	@Order(72)
	@DisplayName("ALTER TYPE RENAME rejects unknown and duplicate field names")
	void testAlterTypeRenameInvalidFields() {
		session.execute("CREATE TYPE IF NOT EXISTS types.alter_rename_bad (street text, city text)");

		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE types.alter_rename_bad RENAME nope TO road"));
		assertDoesNotThrow(
			() -> session.execute("ALTER TYPE types.alter_rename_bad RENAME IF EXISTS nope TO road"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE types.alter_rename_bad RENAME street TO city"));

		assertEquals(List.of(CqlIdentifier.fromInternal("street"), CqlIdentifier.fromInternal("city")),
			userDefinedType("alter_rename_bad").getFieldNames());
	}

	@Test
	@Order(73)
	@DisplayName("Altering the type of a field is no longer supported")
	void testAlterTypeAlterFieldUnsupported() {
		session.execute("CREATE TYPE IF NOT EXISTS types.alter_field (street text)");

		assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TYPE types.alter_field ALTER street TYPE int"));
	}

	private void createUdtLiteralTable() {
		session.execute("CREATE TYPE IF NOT EXISTS types.lit_addr (street text, zip int)");
		session.execute("CREATE TABLE IF NOT EXISTS types.lit_people "
			+ "(id int PRIMARY KEY, home frozen<lit_addr>)");
	}

	@Test
	@Order(74)
	@DisplayName("A UDT literal in an INSERT round-trips")
	void testInsertUdtLiteral() {
		createUdtLiteralTable();

		session.execute(
			"INSERT INTO types.lit_people (id, home) VALUES (1, {street: 'Main', zip: 12345})");

		final var home = session.execute("SELECT home FROM types.lit_people WHERE id = 1")
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

		session.execute("INSERT INTO types.lit_people (id, home) VALUES (2, {street: 'Elm'})");

		final var home = session.execute("SELECT home FROM types.lit_people WHERE id = 2")
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
			"INSERT INTO types.lit_people (id, home) VALUES (3, {street: 'Oak', nope: 1})"));
	}

	@Test
	@Order(77)
	@DisplayName("A UDT literal nests other UDT literals and bind markers")
	void testInsertNestedUdtLiteral() {
		session.execute("CREATE TYPE IF NOT EXISTS types.lit_inner (city text)");
		session.execute(
			"CREATE TYPE IF NOT EXISTS types.lit_outer (street text, place frozen<lit_inner>)");
		session.execute("CREATE TABLE IF NOT EXISTS types.lit_nested "
			+ "(id int PRIMARY KEY, home frozen<lit_outer>)");

		final var prepared = session.prepare(
			"INSERT INTO types.lit_nested (id, home) VALUES (?, {street: ?, place: {city: 'Anytown'}})");

		// A marker inside a UDT literal is typed as the field it stands in for, and named column.field.
		final var variables = prepared.getVariableDefinitions();
		assertEquals(2, variables.size());
		assertEquals(DataTypes.INT, variables.get(0).getType());
		assertEquals(DataTypes.TEXT, variables.get(1).getType());
		assertEquals("home.street", variables.get(1).getName().asInternal());

		session.execute(prepared.bind(1, "Main"));

		final var home = session.execute("SELECT home FROM types.lit_nested WHERE id = 1")
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
		session.execute("INSERT INTO types.lit_people (id, home) VALUES (10, {street: 'Oak', zip: 9})");

		session.execute("UPDATE types.lit_people SET home = {street: 'Pine', zip: 7} WHERE id = 10");
		assertEquals("Pine", session.execute("SELECT home FROM types.lit_people WHERE id = 10")
			.one()
			.getUdtValue("home")
			.getString("street"));

		final var rejected = session.execute("UPDATE types.lit_people SET home = {street: 'Elm'} "
			+ "WHERE id = 10 IF home = {street: 'Oak', zip: 9}");
		assertFalse(rejected.wasApplied());

		final var applied = session.execute("UPDATE types.lit_people SET home = {street: 'Elm'} "
			+ "WHERE id = 10 IF home = {street: 'Pine', zip: 7}");
		assertTrue(applied.wasApplied());
	}

	@Test
	@Order(79)
	@DisplayName("A UDT literal can select and delete by a UDT partition key")
	void testSelectAndDeleteByUdtLiteral() {
		session.execute("CREATE TYPE IF NOT EXISTS types.lit_addr (street text, zip int)");
		session.execute("CREATE TABLE IF NOT EXISTS types.lit_keyed "
			+ "(home frozen<lit_addr> PRIMARY KEY, note text)");
		session.execute(
			"INSERT INTO types.lit_keyed (home, note) VALUES ({street: 'Main', zip: 1}, 'a')");

		final var row = session.execute(
			"SELECT note FROM types.lit_keyed WHERE home = {street: 'Main', zip: 1}").one();
		assertNotNull(row);
		assertEquals("a", row.getString("note"));

		session.execute("DELETE FROM types.lit_keyed WHERE home = {street: 'Main', zip: 1}");
		assertNull(session.execute(
			"SELECT note FROM types.lit_keyed WHERE home = {street: 'Main', zip: 1}").one());
	}

	@Test
	@Order(145)
	@DisplayName("DROP TYPE refuses a type a table or another type still uses")
	void testDropType() {
		session.execute("CREATE TYPE IF NOT EXISTS types.drop_addr (street text)");
		session.execute("CREATE TYPE IF NOT EXISTS types.drop_holder (home frozen<drop_addr>)");
		session.execute("CREATE TABLE IF NOT EXISTS types.drop_type_users "
			+ "(id int PRIMARY KEY, homes list<frozen<drop_addr>>)");

		// A nested reference counts: the type is inside another type and inside a list column.
		assertMentions("drop_holder", assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP TYPE types.drop_addr")));
		session.execute("DROP TYPE types.drop_holder");
		assertMentions("drop_type_users", assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP TYPE types.drop_addr")));

		// A type nothing names is dropped and gone from the metadata.
		session.execute("CREATE TYPE IF NOT EXISTS types.drop_unused (v text)");
		session.execute("DROP TYPE types.drop_unused");
		assertFalse(session.getMetadata()
			.getKeyspace("types")
			.orElseThrow()
			.getUserDefinedType("drop_unused")
			.isPresent());

		// A type that is not there is named in the failure, and forgiven by IF EXISTS.
		assertMentions("drop_unused", assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP TYPE types.drop_unused")));
		assertDoesNotThrow(() -> session.execute("DROP TYPE IF EXISTS types.drop_unused"));
		assertMentions("nope", assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP TYPE nope.drop_addr")));
		assertDoesNotThrow(() -> session.execute("DROP TYPE IF EXISTS nope.drop_addr"));
	}

	@Test
	@Order(245)
	@DisplayName("A UDT and its value answer allIndicesOf; only the value rejects an unknown field")
	void testUdtAllIndicesOf() {
		createUdtLiteralTable();
		session.execute(
			"INSERT INTO types.lit_people (id, home) VALUES (245, {street: 'Main', zip: 1})");

		final var home = session.execute("SELECT home FROM types.lit_people WHERE id = 245")
			.one()
			.getUdtValue("home");
		assertEquals(List.of(1), home.allIndicesOf("zip"));
		assertThrows(IllegalArgumentException.class, () -> home.allIndicesOf("nope"));

		final var type = home.getType();
		assertEquals(List.of(0), type.allIndicesOf("street"));
		assertEquals(List.of(), type.allIndicesOf("nope"));
	}

}
