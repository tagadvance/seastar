package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * Prepared and bound statements: the metadata a prepare exposes, the values a bound statement
 * carries, partition key indices and routing keys. This group owns the {@code prepared} and
 * {@code meta} keyspaces.
 */
public abstract class AbstractPreparedFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("prepared");
	}

	private static final UUID ANN_ID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
	private static final UUID BOB_ID = UUID.fromString("223e4567-e89b-12d3-a456-426614174001");

	private void createMetaTable(final String table) {
		createKeyspace("meta");
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

		// Also with no markers at all: zero declared variables still bounds the value count.
		final var noMarkers = session.prepare("SELECT * FROM meta.people");
		assertThrows(IllegalArgumentException.class, () -> noMarkers.bind("extra"));
	}

	private void createSimpleTable() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS prepared.simple (id int PRIMARY KEY, name text, age int)");
	}

	@Test
	@Order(54)
	@DisplayName("BoundStatement round-trips routing keyspace, idempotence, and custom payload")
	void testBoundStatementMetadata() {
		createSimpleTable();
		final var prepared = session.prepare("SELECT * FROM prepared.simple WHERE id = ?");

		final var payload = Map.of("k", ByteBuffer.wrap(new byte[]{1, 2, 3}));
		final var bound = prepared.bind(1)
			.setRoutingKeyspace(CqlIdentifier.fromInternal("prepared"))
			.setIdempotent(true)
			.setCustomPayload(payload);

		assertEquals(CqlIdentifier.fromInternal("prepared"), bound.getRoutingKeyspace());
		assertEquals(Boolean.TRUE, bound.isIdempotent());
		assertEquals(payload, bound.getCustomPayload());
	}

	private void createCompositeKeyTable() {
		session.execute("CREATE TABLE IF NOT EXISTS prepared.composite "
			+ "(pk1 int, pk2 int, cc int, v int, PRIMARY KEY ((pk1, pk2), cc))");
	}

	@Test
	@Order(57)
	@DisplayName("Partition key indices are ordered by partition key position, not bind order")
	void testPartitionKeyIndicesOrderedByPosition() {
		createCompositeKeyTable();

		final var inOrder = session.prepare(
			"SELECT v FROM prepared.composite WHERE pk1 = ? AND pk2 = ? AND cc = ?");
		assertEquals(List.of(0, 1), inOrder.getPartitionKeyIndices());

		// pk2 is bound first, but pk1 is the first partition key component.
		final var reversed = session.prepare(
			"SELECT v FROM prepared.composite WHERE pk2 = ? AND pk1 = ? AND cc = ?");
		assertEquals(List.of(1, 0), reversed.getPartitionKeyIndices());
	}

	@Test
	@Order(58)
	@DisplayName("Partition key indices are empty unless every component is a bind marker")
	void testPartitionKeyIndicesRequireEveryComponent() {
		createCompositeKeyTable();

		final var hardCoded = session.prepare(
			"UPDATE prepared.composite SET v = ? WHERE pk1 = 1 AND pk2 = ? AND cc = ?");
		assertEquals(List.of(), hardCoded.getPartitionKeyIndices());

		final var bound = session.prepare(
			"UPDATE prepared.composite SET v = ? WHERE pk1 = ? AND pk2 = ? AND cc = ?");
		assertEquals(List.of(1, 2), bound.getPartitionKeyIndices());
	}

	@Test
	@Order(59)
	@DisplayName("INSERT exposes partition key indices from the VALUES clause")
	void testPartitionKeyIndicesForInsert() {
		createCompositeKeyTable();

		final var prepared = session.prepare(
			"INSERT INTO prepared.composite (cc, pk2, v, pk1) VALUES (?, ?, ?, ?)");

		assertEquals(List.of(3, 1), prepared.getPartitionKeyIndices());
	}

	@Test
	@Order(60)
	@DisplayName("A statement with no partition key markers has no partition key indices")
	void testPartitionKeyIndicesWithoutMarkers() {
		createCompositeKeyTable();

		final var prepared = session.prepare(
			"SELECT v FROM prepared.composite WHERE pk1 = 1 AND pk2 = 2");

		assertEquals(List.of(), prepared.getPartitionKeyIndices());
	}

	@Test
	@Order(61)
	@DisplayName("A LIMIT marker does not shift partition key indices")
	void testPartitionKeyIndicesIgnoreLimitMarker() {
		createCompositeKeyTable();

		final var prepared = session.prepare(
			"SELECT v FROM prepared.composite WHERE pk1 = ? AND pk2 = ? LIMIT ?");

		assertEquals(List.of(0, 1), prepared.getPartitionKeyIndices());
	}

	@Test
	@Order(62)
	@DisplayName("The routing key of a single-component partition key is the encoded value")
	void testRoutingKeySingleComponent() {
		createSimpleTable();

		final var bound = session.prepare("SELECT name FROM prepared.simple WHERE id = ?").bind(7);

		assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0, 7}), bound.getRoutingKey());
	}

	@Test
	@Order(63)
	@DisplayName("A composite routing key is length-prefixed in partition key order")
	void testRoutingKeyCompositeComponents() {
		createCompositeKeyTable();

		final var bound = session.prepare(
			"SELECT v FROM prepared.composite WHERE pk2 = ? AND pk1 = ? AND cc = ?").bind(9, 7, 1);

		// Two-byte length, value, then a zero byte per component; pk1 precedes pk2 despite bind order.
		final var expected = ByteBuffer.wrap(new byte[]{0, 4, 0, 0, 0, 7, 0, 0, 4, 0, 0, 0, 9, 0});
		assertEquals(expected, bound.getRoutingKey());
	}

	@Test
	@Order(64)
	@DisplayName("An explicitly set routing key overrides the partition key")
	void testRoutingKeyExplicitOverride() {
		createSimpleTable();

		final var override = ByteBuffer.wrap(new byte[]{1, 2, 3});
		final var bound = session.prepare("SELECT name FROM prepared.simple WHERE id = ?")
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
			"SELECT v FROM prepared.composite WHERE pk1 = 1 AND pk2 = ? AND cc = ?").bind(9, 1);

		assertNull(bound.getRoutingKey());
	}

	@Test
	@Order(66)
	@DisplayName("The routing key is null when a partition key value is unset")
	void testRoutingKeyWithUnsetValue() {
		createCompositeKeyTable();

		final var bound = session.prepare(
			"SELECT v FROM prepared.composite WHERE pk1 = ? AND pk2 = ? AND cc = ?").bind();

		assertNull(bound.getRoutingKey());
	}

	@Test
	@Order(94)
	@DisplayName("Binding a value its column cannot hold throws CodecNotFoundException at bind time")
	void testBoundValueTypeChecking() {
		session.execute("CREATE TABLE IF NOT EXISTS prepared.bound "
			+ "(id int PRIMARY KEY, name text, tags list<int>)");
		final var prepared = session.prepare(
			"INSERT INTO prepared.bound (id, name, tags) VALUES (?, ?, ?)");

		assertThrows(CodecNotFoundException.class, () -> prepared.bind(1, 2, List.of(3)));
		assertThrows(CodecNotFoundException.class, () -> prepared.bind(1L, "Ann", List.of(3)));
		assertThrows(CodecNotFoundException.class, () -> prepared.bind(1, "Ann", List.of("x")));

		assertDoesNotThrow(() -> session.execute(prepared.bind(1, "Ann", List.of(3))));
		// A null binds to any column, and trailing markers may be left unbound.
		assertDoesNotThrow(() -> session.execute(prepared.bind(2, null, null)));
		assertDoesNotThrow(() -> session.execute(prepared.bind(3)));

		final var row = session.execute("SELECT * FROM prepared.bound WHERE id = 1").one();
		assertNotNull(row);
		assertEquals("Ann", row.getString("name"));
		assertEquals(List.of(3), row.getList("tags", Integer.class));
	}

	@Test
	@Order(96)
	@DisplayName("prepare rejects a statement naming a keyspace, table or column that does not exist")
	void testPrepareValidatesAgainstTheSchema() {
		session.execute("CREATE TABLE IF NOT EXISTS prepared.prep (id int PRIMARY KEY, name text)");

		assertThrows(InvalidQueryException.class,
			() -> session.prepare("SELECT * FROM prepared.no_such_table WHERE id = ?"));
		assertThrows(InvalidQueryException.class,
			() -> session.prepare("SELECT * FROM no_such_keyspace.prep WHERE id = ?"));
		assertThrows(InvalidQueryException.class,
			() -> session.prepare("INSERT INTO prepared.prep (id, no_such_col) VALUES (?, ?)"));
		assertThrows(InvalidQueryException.class,
			() -> session.prepare("SELECT no_such_col FROM prepared.prep WHERE id = ?"));
		assertThrows(InvalidQueryException.class,
			() -> session.prepare("UPDATE prepared.prep SET no_such_col = ? WHERE id = ?"));
		assertThrows(SyntaxError.class, () -> session.prepare("SELECT FROM WHERE"));

		// A statement that addresses no table carries no markers, and preparing it succeeds.
		assertDoesNotThrow(() -> session.prepare("TRUNCATE prepared.prep"));
		assertDoesNotThrow(() -> session.prepare("SELECT * FROM prepared.prep WHERE id = ?"));
	}

	@Test
	@Order(97)
	@DisplayName("getResultMetadataId returns a readable, stable, read-only identifier")
	void testResultMetadataIdIsReadable() {
		assumeTrue(hasResultMetadataId(),
			"the result metadata id arrived with native protocol v5; this backend is reached over v4");

		session.execute("CREATE TABLE IF NOT EXISTS prepared.meta_id (id int PRIMARY KEY, name text)");
		final var prepared = session.prepare("SELECT * FROM prepared.meta_id WHERE id = ?");

		final var id = prepared.getResultMetadataId();
		assertTrue(id.remaining() > 0, "the identifier must not be an empty buffer");
		assertTrue(id.isReadOnly());

		// The id is opaque but stable: asking twice describes the same result metadata.
		assertEquals(id, prepared.getResultMetadataId());
	}

	@Test
	@Order(243)
	@DisplayName("A bound statement answers allIndicesOf and rejects an unknown variable")
	void testBoundStatementAllIndicesOf() {
		createMetaTable("people");
		final var bound = session.prepare("SELECT * FROM meta.people WHERE id = ?").bind(ANN_ID);

		assertEquals(List.of(0), bound.allIndicesOf("id"));
		assertEquals(List.of(0), bound.allIndicesOf(CqlIdentifier.fromCql("id")));
		assertThrows(IllegalArgumentException.class, () -> bound.allIndicesOf("nope"));
		assertThrows(IllegalArgumentException.class,
			() -> bound.allIndicesOf(CqlIdentifier.fromCql("nope")));
	}

}
