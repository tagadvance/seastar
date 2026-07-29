package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The UDT half of the {@code system_schema} projection, assembled by the driver rather than
 * inspected as strings.
 *
 * <p>{@code SystemSchemaTest} pins the type strings the projection writes, and runs them back
 * through {@code DataTypeCqlNameParser} - the parser the driver actually uses. What it cannot reach
 * is the step after that: {@code UserDefinedTypeParser} topologically sorting a keyspace's types,
 * resolving each one's fields against the types it already built, and handing the result to
 * {@code TableParser} so a column can name it. Nothing here had ever made a real {@link CqlSession}
 * do that, which is the gap this closes.
 *
 * <p>The type string is the only place the projection quotes an identifier - {@code column_name},
 * {@code table_name}, {@code type_name} and {@code field_names} are all raw - because the driver
 * reads a name back with {@code CqlIdentifier#fromCql} there and {@code fromInternal} everywhere
 * else. It is also asymmetric with the driver's own {@code UserDefinedType#asCql}, which qualifies
 * the type with its keyspace and which {@code DataTypeCqlNameParser} then refuses to parse. Both
 * halves are exercised below.
 *
 * <p><strong>The session is stock</strong> - contact point and datacenter, nothing else - because
 * an unconfigured production driver is the thing under test. That costs the driver's own schema
 * debounce window - a second per DDL statement, and nothing to do with SeaStar - so the schema is
 * seeded once for the whole class rather than per test. {@code WireCqlSessionTest} buys the same
 * second back by shortening the window instead, because it runs hundreds of statements.
 */
@NullMarked
class DriverUserDefinedTypeTest {

	private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("udt");
	private static final CqlIdentifier ADDRESS = CqlIdentifier.fromInternal("address");
	private static final CqlIdentifier CONTACT = CqlIdentifier.fromInternal("contact");
	private static final CqlIdentifier CASED = CqlIdentifier.fromInternal("Address");

	private static SeaStarCqlSession backing;
	private static SeaStarProtocolServer server;
	private static CqlSession session;

	@BeforeAll
	static void setUp() {
		backing = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(backing).build().start();
		session = CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()))
			.withLocalDatacenter("datacenter1")
			.build();

		session.execute("CREATE KEYSPACE udt WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}");
		session.execute("CREATE TYPE udt.address (street text, zip int)");
		// Nested: a field whose type is another user type, which is what makes the driver sort the
		// keyspace's types before it parses them.
		session.execute("CREATE TYPE udt.contact (name text, home frozen<address>)");
		// Quoted type name and quoted field name. `zip` stays unquoted on purpose, so a projection
		// that quoted every field name would fail rather than pass by accident.
		session.execute("CREATE TYPE udt.\"Address\" (\"Street\" text, zip int)");
		session.execute("CREATE TABLE udt.people (id int PRIMARY KEY, home frozen<address>, "
			+ "places map<text, frozen<address>>, who frozen<contact>, cased frozen<\"Address\">, "
			+ "bare \"Address\")");
	}

	@AfterAll
	static void tearDown() {
		session.close();
		server.close();
		backing.close();
	}

	@Test
	@DisplayName("the driver builds every user-defined type of the keyspace from system_schema")
	void testTypesAreParsed() {
		final var types = keyspace().getUserDefinedTypes();

		assertEquals(List.of("Address", "address", "contact"),
			types.keySet().stream().map(CqlIdentifier::asInternal).sorted().toList());
		types.forEach((name, type) -> {
			assertEquals(KEYSPACE, type.getKeyspace());
			// A keyspace's own types are always unfrozen; frozenness belongs to the use, not the
			// declaration, and the driver's parser hardcodes false here.
			assertFalse(type.isFrozen(), "a declared type is not frozen");
		});

		final var address = type(ADDRESS);
		assertEquals(List.of("street", "zip"), fieldNames(address));
		assertEquals(List.of(DataTypes.TEXT, DataTypes.INT), address.getFieldTypes());
	}

	@Test
	@DisplayName("a quoted type name and a quoted field name survive the round trip in their own case")
	void testQuotedNames() {
		final var cased = type(CASED);

		assertEquals("Address", cased.getName().asInternal());
		assertEquals("\"Address\"", cased.getName().asCql(true));
		assertEquals(List.of("Street", "zip"), fieldNames(cased));
		assertEquals(List.of(DataTypes.TEXT, DataTypes.INT), cased.getFieldTypes());
		// The lower-cased name is a different type, and the driver keeps them apart.
		assertEquals(List.of("street", "zip"), fieldNames(type(ADDRESS)));
	}

	@Test
	@DisplayName("a type whose field is another type resolves to that type, frozen")
	void testNestedType() {
		final var contact = type(CONTACT);

		assertEquals(List.of("name", "home"), fieldNames(contact));
		assertEquals(DataTypes.TEXT, contact.getFieldTypes().get(0));

		final var home = assertInstanceOf(UserDefinedType.class, contact.getFieldTypes().get(1));
		assertEquals(type(ADDRESS), home);
		assertTrue(home.isFrozen(), "a user type nested in another is frozen");
		assertEquals(List.of("street", "zip"), fieldNames(home));
	}

	@Test
	@DisplayName("a table column names the type the keyspace declared, frozen or not")
	void testColumnTypes() {
		final var table = table();

		final var home = assertInstanceOf(UserDefinedType.class, columnType(table, "home"));
		assertEquals(type(ADDRESS), home);
		assertTrue(home.isFrozen());

		final var who = assertInstanceOf(UserDefinedType.class, columnType(table, "who"));
		assertEquals(type(CONTACT), who);

		final var cased = assertInstanceOf(UserDefinedType.class, columnType(table, "cased"));
		assertEquals(type(CASED), cased);
		assertTrue(cased.isFrozen());

		// The one column declared without the keyword. Frozenness is not on the wire and is not part
		// of DataType equality, so it is only ever visible through isFrozen().
		final var bare = assertInstanceOf(UserDefinedType.class, columnType(table, "bare"));
		assertEquals(type(CASED), bare);
		assertFalse(bare.isFrozen(), "a column declared without frozen<> is not frozen");
	}

	@Test
	@DisplayName("a user-defined type frozen inside a collection column is parsed as the value type")
	void testTypeInsideACollection() {
		final var places = assertInstanceOf(MapType.class, columnType(table(), "places"));

		assertEquals(DataTypes.TEXT, places.getKeyType());
		final var value = assertInstanceOf(UserDefinedType.class, places.getValueType());
		assertEquals(type(ADDRESS), value);
		assertTrue(value.isFrozen(), "a user type inside a collection is frozen");
		assertFalse(places.isFrozen(), "the collection itself is not");
	}

	@Test
	@DisplayName("the driver's own asCql for a type is the qualified form the projection must not write")
	void testAsCqlIsNotWhatTheProjectionWrites() {
		// SystemSchema#cqlType writes the recursion out by hand rather than delegating, and this is
		// why: the driver qualifies a user type with its keyspace, and its own schema parser then
		// rejects that form with "Can't find referenced user type". The projection writing what
		// asCql produces would leave every keyspace containing a UDT without metadata at all.
		assertEquals("udt.address", type(ADDRESS).asCql(true, true));
		assertEquals("udt.\"Address\"", type(CASED).asCql(true, true));
	}

	@Test
	@DisplayName("a row written and read through the metadata's types round-trips over the wire")
	void testValueRoundTrip() {
		final var address = type(ADDRESS);
		final var main = address.newValue().setString("street", "Main").setInt("zip", 12345);
		final var elm = address.newValue().setString("street", "Elm").setInt("zip", 67890);
		final var who = type(CONTACT).newValue().setString("name", "Ada").setUdtValue("home", main);
		final var cased = type(CASED).newValue().setString("Street", "Oak").setInt("zip", 3);

		final var insert = session.prepare("INSERT INTO udt.people "
			+ "(id, home, places, who, cased, bare) VALUES (?, ?, ?, ?, ?, ?)");
		session.execute(insert.bind(1, main, Map.of("work", elm), who, cased, cased));

		final var row = session.execute("SELECT * FROM udt.people WHERE id = 1").one();
		assertNotNull(row);
		assertEquals(main, row.getUdtValue("home"));
		assertEquals("Main", row.getUdtValue("home").getString("street"));
		assertEquals(Map.of("work", elm),
			row.getMap("places", String.class, UdtValue.class));
		assertEquals("Ada", row.getUdtValue("who").getString("name"));
		assertEquals(12345, row.getUdtValue("who").getUdtValue("home").getInt("zip"));
		assertEquals("Oak", row.getUdtValue("cased").getString("Street"));
		assertEquals("Oak", row.getUdtValue("bare").getString("Street"));
	}

	@Test
	@DisplayName("a UDT literal written in CQL reads back through the driver's metadata codec")
	void testLiteralRoundTrip() {
		session.execute("INSERT INTO udt.people (id, home, cased) VALUES "
			+ "(2, {street: 'Pine', zip: 7}, {\"Street\": 'Birch', zip: 8})");

		final var row = session.execute("SELECT home, cased FROM udt.people WHERE id = 2").one();
		assertNotNull(row);
		assertEquals("Pine", row.getUdtValue("home").getString("street"));
		assertEquals("Birch", row.getUdtValue("cased").getString("Street"));
	}

	private static KeyspaceMetadata keyspace() {
		return session.getMetadata()
			.getKeyspace(KEYSPACE)
			.orElseThrow(() -> new AssertionError(
				"the keyspace comes from system_schema; a type the driver cannot parse abandons the "
					+ "whole refresh and leaves it absent"));
	}

	private static UserDefinedType type(final CqlIdentifier name) {
		return keyspace().getUserDefinedType(name)
			.orElseThrow(() -> new AssertionError("no user-defined type " + name.asInternal()));
	}

	private static TableMetadata table() {
		return keyspace().getTable(CqlIdentifier.fromInternal("people"))
			.orElseThrow(() -> new AssertionError("no table people"));
	}

	private static DataType columnType(final TableMetadata table, final String column) {
		return table.getColumn(CqlIdentifier.fromInternal(column))
			.orElseThrow(() -> new AssertionError("no column " + column))
			.getType();
	}

	private static List<String> fieldNames(final UserDefinedType type) {
		return type.getFieldNames().stream().map(CqlIdentifier::asInternal).toList();
	}
}
