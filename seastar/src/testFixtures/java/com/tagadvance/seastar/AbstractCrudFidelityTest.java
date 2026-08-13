package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * The data statements at their plainest: INSERT, SELECT, UPDATE and DELETE against a
 * single-partition-key table. This group owns the {@code crud} keyspace.
 */
public abstract class AbstractCrudFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("crud");
	}

	@Test
	@Order(4)
	@DisplayName("SELECT * returns all columns addressable by index, name, and identifier")
	void testSimpleSelect() {
		session.execute("CREATE TABLE IF NOT EXISTS crud.bar (foo text PRIMARY KEY, bar text)");
		session.execute("INSERT INTO crud.bar (foo, bar) VALUES ('foo', 'bar')");

		final var resultSet = session.execute("SELECT * FROM crud.bar");
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

	private static final UUID ANN_ID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
	private static final UUID BOB_ID = UUID.fromString("223e4567-e89b-12d3-a456-426614174001");
	private static final UUID CAROL_ID = UUID.fromString("323e4567-e89b-12d3-a456-426614174002");

	private String nameOf(final UUID id) {
		return session.execute("SELECT * FROM crud.people").all().stream()
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
			"CREATE TABLE IF NOT EXISTS crud.people (id uuid PRIMARY KEY, name text);");

		final var prepared = session.prepare(
			"INSERT INTO crud.people (id, name) VALUES (?, ?)");
		final var resultSet = session.execute(prepared.bind(ANN_ID, "Ann"));
		assertNotNull(resultSet);

		assertEquals("Ann", nameOf(ANN_ID));
	}

	@Test
	@Order(9)
	@DisplayName("INSERT with literal values stores a row readable by SELECT")
	void testInsertWithLiterals() {
		final var resultSet = session.execute(
			"INSERT INTO crud.people (id, name) VALUES (223e4567-e89b-12d3-a456-426614174001, 'Bob')");
		assertNotNull(resultSet);

		assertEquals("Bob", nameOf(BOB_ID));
	}

	@Test
	@Order(10)
	@DisplayName("INSERT IF NOT EXISTS does not overwrite an existing row")
	void testInsertIfNotExists() {
		session.execute(
			"INSERT INTO crud.people (id, name) VALUES (323e4567-e89b-12d3-a456-426614174002, 'Carol') IF NOT EXISTS");
		session.execute(
			"INSERT INTO crud.people (id, name) VALUES (323e4567-e89b-12d3-a456-426614174002, 'Dave') IF NOT EXISTS");

		assertEquals("Carol", nameOf(CAROL_ID));
	}

	@Test
	@Order(11)
	@DisplayName("INSERT into an unknown table throws InvalidQueryException")
	void testInsertUnknownTable() {
		assertThrows(InvalidQueryException.class, () -> session.execute(
			"INSERT INTO crud.nope (id, name) VALUES (323e4567-e89b-12d3-a456-426614174002, 'x')"));
	}

	@Test
	@Order(12)
	@DisplayName("INSERT omitting the primary key throws InvalidQueryException")
	void testInsertMissingPrimaryKey() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO crud.people (name) VALUES ('Ann')"));
	}

	@Test
	@Order(13)
	@DisplayName("SELECT with a column list projects only the selected columns")
	void testSelectProjection() {
		final var resultSet = session.execute("SELECT name FROM crud.people WHERE id = " + ANN_ID);

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
		final var prepared = session.prepare("SELECT * FROM crud.people WHERE id = ?");
		final var all = session.execute(prepared.bind(BOB_ID)).all();

		assertEquals(1, all.size());
		assertEquals("Bob", all.get(0).getString("name"));
	}

	@Test
	@Order(15)
	@DisplayName("SELECT WHERE IN returns every matching row")
	void testSelectWhereIn() {
		final var prepared = session.prepare("SELECT * FROM crud.people WHERE id IN (?, ?)");
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
			() -> session.execute("SELECT * FROM crud.people WHERE name = 'Ann'"));

		final var all = session.execute(
			"SELECT * FROM crud.people WHERE name = 'Ann' ALLOW FILTERING").all();
		assertEquals(1, all.size());
		assertEquals("Ann", all.get(0).getString("name"));
	}

	@Test
	@Order(17)
	@DisplayName("SELECT LIMIT caps the row count and rejects a non-positive limit")
	void testSelectLimit() {
		assertEquals(1, session.execute("SELECT * FROM crud.people LIMIT 1").all().size());

		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT * FROM crud.people LIMIT 0"));
	}

	@Test
	@Order(18)
	@DisplayName("SELECT of an unknown column throws InvalidQueryException")
	void testSelectUnknownColumn() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT nope FROM crud.people"));
	}

	private static final UUID EVE_ID = UUID.fromString("423e4567-e89b-12d3-a456-426614174003");

	@Test
	@Order(19)
	@DisplayName("UPDATE SET on an existing row changes the value")
	void testUpdateExistingRow() {
		final var prepared = session.prepare(
			"UPDATE crud.people SET name = ? WHERE id = ?");
		session.execute(prepared.bind("Annette", ANN_ID));

		assertEquals("Annette", nameOf(ANN_ID));
	}

	@Test
	@Order(20)
	@DisplayName("UPDATE of a non-existent primary key upserts a new row")
	void testUpdateUpsert() {
		assertNull(nameOf(EVE_ID));

		session.execute(
			"UPDATE crud.people SET name = 'Eve' WHERE id = 423e4567-e89b-12d3-a456-426614174003");

		assertEquals("Eve", nameOf(EVE_ID));
	}

	@Test
	@Order(21)
	@DisplayName("UPDATE setting a primary key column throws InvalidQueryException")
	void testUpdatePrimaryKeyInSet() {
		assertThrows(InvalidQueryException.class, () -> session.execute(
			"UPDATE crud.people SET id = " + BOB_ID + " WHERE id = " + ANN_ID));
	}

	@Test
	@Order(22)
	@DisplayName("UPDATE restricting a non-primary-key column throws InvalidQueryException")
	void testUpdateNonKeyWhere() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("UPDATE crud.people SET name = 'x' WHERE name = 'Bob'"));
	}

	private static final UUID FRANK_ID = UUID.fromString("523e4567-e89b-12d3-a456-426614174004");
	private static final UUID GRACE_ID = UUID.fromString("623e4567-e89b-12d3-a456-426614174005");

	@Test
	@Order(23)
	@DisplayName("DELETE by primary key removes only the matching row")
	void testDeleteByPrimaryKey() {
		session.execute("INSERT INTO crud.people (id, name) VALUES (" + FRANK_ID + ", 'Frank')");
		assertEquals("Frank", nameOf(FRANK_ID));

		final var prepared = session.prepare("DELETE FROM crud.people WHERE id = ?");
		session.execute(prepared.bind(FRANK_ID));

		assertNull(nameOf(FRANK_ID));
		assertEquals("Bob", nameOf(BOB_ID));
	}

	@Test
	@Order(24)
	@DisplayName("DELETE of a named column nulls it but keeps the row")
	void testDeleteColumn() {
		session.execute("INSERT INTO crud.people (id, name) VALUES (" + GRACE_ID + ", 'Grace')");

		session.execute("DELETE name FROM crud.people WHERE id = " + GRACE_ID);

		final var rows = session.execute("SELECT * FROM crud.people WHERE id = " + GRACE_ID).all();
		assertEquals(1, rows.size());
		assertNull(rows.get(0).getString("name"));
	}

	@Test
	@Order(25)
	@DisplayName("DELETE restricting a non-primary-key column throws InvalidQueryException")
	void testDeleteNonKeyWhere() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("DELETE FROM crud.people WHERE name = 'Bob'"));
	}

	@Test
	@Order(26)
	@DisplayName("DELETE naming a primary key column throws InvalidQueryException")
	void testDeletePrimaryKeyColumn() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("DELETE id FROM crud.people WHERE id = " + BOB_ID));
	}

	@Test
	@Order(27)
	@DisplayName("DELETE from an unknown table throws InvalidQueryException")
	void testDeleteUnknownTable() {
		assertThrows(InvalidQueryException.class,
			() -> session.execute("DELETE FROM crud.nope WHERE id = " + BOB_ID));
	}

}
