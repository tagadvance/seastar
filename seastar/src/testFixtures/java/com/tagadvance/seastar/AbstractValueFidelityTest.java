package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * The values a statement carries: identifier case rules, the literal forms and what a column will
 * not take, a {@code SimpleStatement}'s own positional and named values, and the bind markers a
 * statement carries outside its columns. This group owns the {@code vals} keyspace.
 */
public abstract class AbstractValueFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("vals");
	}

	@Test
	@Order(55)
	@DisplayName("Quoted identifiers are case-sensitive; unquoted references fold to lower case")
	void testQuotedIdentifierCaseSensitivity() {
		session.execute("CREATE TABLE vals.quoting (id int PRIMARY KEY, \"MixedCase\" text)");
		session.execute("INSERT INTO vals.quoting (id, \"MixedCase\") VALUES (1, 'v')");

		final var row = session.execute("SELECT \"MixedCase\" FROM vals.quoting WHERE id = 1").one();
		assertEquals("v", row.getString(0));

		// An unquoted reference folds to lower case ("mixedcase"), which is not a defined column.
		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT mixedcase FROM vals.quoting WHERE id = 1"));
	}

	@Test
	@Order(80)
	@DisplayName("A quoted column keeps its case while an unquoted one folds to lower case")
	void testCaseSensitiveColumnNames() {
		session.execute("CREATE TABLE IF NOT EXISTS vals.cased "
			+ "(id int PRIMARY KEY, \"myColumn\" int, MixedCase int)");
		session.execute("INSERT INTO vals.cased (id, \"myColumn\", MixedCase) VALUES (1, 2, 3)");

		// An unquoted identifier folds to lower case wherever it appears, select clause included.
		final var folded = session.execute("SELECT MixedCase FROM vals.cased WHERE ID = 1").one();
		assertNotNull(folded);
		assertEquals(3, folded.getInt("mixedcase"));

		// A quoted identifier is matched exactly, so the two spellings are different columns.
		final var quoted = session.execute("SELECT \"myColumn\" FROM vals.cased WHERE id = 1").one();
		assertNotNull(quoted);
		assertEquals(2, quoted.getInt("\"myColumn\""));

		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT myColumn FROM vals.cased WHERE id = 1"));
	}

	@Test
	@Order(88)
	@DisplayName("Collection, tuple and vector literals insert and read back as their Java values")
	void testCollectionLiterals() {
		session.execute("CREATE TABLE IF NOT EXISTS vals.literals (id int PRIMARY KEY, l list<int>, "
			+ "s set<int>, m map<text, int>, t tuple<int, text>, v vector<float, 2>)");
		session.execute("INSERT INTO vals.literals (id, l, s, m, t, v) VALUES "
			+ "(1, [1, 2], {4, 3}, {'a': 5}, (6, 'x'), [1.5, 2.5])");

		final var row = session.execute("SELECT * FROM vals.literals WHERE id = 1").one();
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
		session.execute("CREATE TABLE IF NOT EXISTS vals.literal_errors "
			+ "(id int PRIMARY KEY, l list<int>, s set<int>, m map<text, int>)");

		// {} is parsed as an empty set because the grammar cannot tell it from an empty map, so a
		// list column rejects it, and [] - which is a list or a vector - is rejected by a set.
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO vals.literal_errors (id, l) VALUES (1, {})"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO vals.literal_errors (id, s) VALUES (1, [])"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO vals.literal_errors (id, s) VALUES (1, {'a': 1})"));
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO vals.literal_errors (id, m) VALUES (1, {1: 1})"));
	}

	@Test
	@Order(90)
	@DisplayName("An empty collection is null unless it is frozen, and its getter still answers empty")
	void testEmptyCollectionLiterals() {
		session.execute("CREATE TABLE IF NOT EXISTS vals.empties (id int PRIMARY KEY, l list<int>, "
			+ "s set<int>, m map<text, int>, fl frozen<list<int>>, fs frozen<set<int>>, "
			+ "fm frozen<map<text, int>>)");
		session.execute(
			"INSERT INTO vals.empties (id, l, s, m, fl, fs, fm) VALUES (1, [], {}, {}, [], {}, {})");

		final var row = session.execute("SELECT * FROM vals.empties WHERE id = 1").one();
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
			"CREATE TABLE IF NOT EXISTS vals.null_literals (id int PRIMARY KEY, name text, tags list<int>)");
		session.execute("INSERT INTO vals.null_literals (id, name, tags) VALUES (1, 'Ann', [1])");
		session.execute("INSERT INTO vals.null_literals (id, name, tags) VALUES (1, null, null)");

		final var row = session.execute("SELECT * FROM vals.null_literals WHERE id = 1").one();
		assertNotNull(row);
		assertNull(row.getString("name"));
		assertTrue(row.isNull("tags"));
	}

	@Test
	@Order(92)
	@DisplayName("now(), uuid() and currentTimestamp() are evaluated when the statement runs")
	void testTermFunctions() {
		session.execute("CREATE TABLE IF NOT EXISTS vals.functions "
			+ "(id int PRIMARY KEY, tu timeuuid, u uuid, ts timestamp)");
		final var before = Instant.now().minusSeconds(60);
		session.execute("INSERT INTO vals.functions (id, tu, u, ts) VALUES "
			+ "(1, now(), uuid(), currentTimestamp())");

		final var row = session.execute("SELECT * FROM vals.functions WHERE id = 1").one();
		assertNotNull(row);
		assertEquals(1, row.getUuid("tu").version());
		assertEquals(4, row.getUuid("u").version());
		assertTrue(row.getInstant("ts").isAfter(before));

		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO vals.functions (id, u) VALUES (2, wat())"));
		// A function whose result the column cannot hold is a type error, not an unknown function.
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO vals.functions (id, tu) VALUES (2, currentTimestamp())"));
	}

	@Test
	@Order(93)
	@DisplayName("A type cast is accepted for the column's own type and rejected for any other")
	void testTypeCast() {
		session.execute("CREATE TABLE IF NOT EXISTS vals.casts (id int PRIMARY KEY, name text)");
		session.execute("INSERT INTO vals.casts (id, name) VALUES ((int) 1, (text) 'Ann')");

		final var row = session.execute("SELECT name FROM vals.casts WHERE id = 1").one();
		assertNotNull(row);
		assertEquals("Ann", row.getString("name"));

		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO vals.casts (id, name) VALUES ((bigint) 2, 'Bob')"));
	}

	// The values a statement carries in its own right, rather than through a prepared statement: what
	// they are bound to, how many of them there have to be, and which of them a column will not take.

	private void createValueTable() {
		session.execute("CREATE TABLE IF NOT EXISTS vals.vals "
			+ "(pk int, ck int, v text, PRIMARY KEY (pk, ck))");
	}

	@Test
	@Order(230)
	@DisplayName("A SimpleStatement's positional values are bound to the markers it was written with")
	void testPositionalValues() {
		createValueTable();

		for (int ck = 1; ck <= 3; ck++) {
			session.execute(SimpleStatement.newInstance(
				"INSERT INTO vals.vals (pk, ck, v) VALUES (?, ?, ?)", 1, ck, "v" + ck));
		}
		// Three rows rather than one: values that are dropped rather than bound leave every insert
		// writing the same all-null key, and the row count is what says so.
		assertEquals(List.of("v1", "v2", "v3"), texts("SELECT v FROM vals.vals WHERE pk = 1"));

		final var selected = session.execute(SimpleStatement.newInstance(
			"SELECT v FROM vals.vals WHERE pk = ? AND ck = ?", 1, 2)).one();
		assertNotNull(selected);
		assertEquals("v2", selected.getString(0));

		session.execute(SimpleStatement.newInstance(
			"UPDATE vals.vals SET v = ? WHERE pk = ? AND ck = ?", "updated", 1, 2));
		assertEquals(List.of("updated", "v1", "v3"), texts("SELECT v FROM vals.vals WHERE pk = 1"));

		session.execute(SimpleStatement.newInstance(
			"DELETE FROM vals.vals WHERE pk = ? AND ck = ?", 1, 3));
		assertEquals(List.of("updated", "v1"), texts("SELECT v FROM vals.vals WHERE pk = 1"));

		// A null outside the primary key is an ordinary value, and clears the column it is bound to.
		session.execute(SimpleStatement.newInstance(
			"INSERT INTO vals.vals (pk, ck, v) VALUES (?, ?, ?)", 1, 1, null));
		final var cleared = session.execute(SimpleStatement.newInstance(
			"SELECT v FROM vals.vals WHERE pk = ? AND ck = ?", 1, 1)).one();
		assertNotNull(cleared);
		assertTrue(cleared.isNull(0), "a null bound to a non-key column should clear it");
	}

	@Test
	@Order(231)
	@DisplayName("Named values are bound by the name of the marker, a ? being named for its column")
	void testNamedValues() {
		createValueTable();

		session.execute(SimpleStatement.builder(
				"INSERT INTO vals.vals (pk, ck, v) VALUES (:pk, :ck, :v)")
			.addNamedValue("pk", 2)
			.addNamedValue("ck", 1)
			.addNamedValue("v", "named")
			.build());
		assertEquals(List.of("named"), texts("SELECT v FROM vals.vals WHERE pk = 2"));

		// An anonymous marker is named after the column it stands for, so a named value reaches it too.
		session.execute(SimpleStatement.builder("INSERT INTO vals.vals (pk, ck, v) VALUES (?, ?, ?)")
			.addNamedValue("pk", 3)
			.addNamedValue("ck", 1)
			.addNamedValue("v", "by column")
			.build());
		assertEquals(List.of("by column"), texts("SELECT v FROM vals.vals WHERE pk = 3"));

		final var selected = session.execute(
			SimpleStatement.builder("SELECT v FROM vals.vals WHERE pk = :pk AND ck = :ck")
				.addNamedValue("pk", 2)
				.addNamedValue("ck", 1)
				.build()).one();
		assertNotNull(selected);
		assertEquals("named", selected.getString(0));

		// A name that is no marker of this statement leaves one of them unaccounted for.
		assertWrongNumberOfValues(SimpleStatement.builder(
				"INSERT INTO vals.vals (pk, ck, v) VALUES (?, ?, ?)")
			.addNamedValue("pk", 4)
			.addNamedValue("ck", 1)
			.addNamedValue("nosuch", "x")
			.build());
	}

	@Test
	@Order(232)
	@DisplayName("Values that do not account for every bind marker are refused, too few or too many")
	void testWrongNumberOfValues() {
		createValueTable();

		final var insert = "INSERT INTO vals.vals (pk, ck, v) VALUES (?, ?, ?)";
		assertWrongNumberOfValues(SimpleStatement.newInstance(insert, 6, 1));
		assertWrongNumberOfValues(SimpleStatement.newInstance(insert, 6, 1, "x", "extra"));
		assertWrongNumberOfValues(SimpleStatement.newInstance(insert));
		assertWrongNumberOfValues(SimpleStatement.builder(insert)
			.addNamedValue("pk", 6)
			.addNamedValue("ck", 1)
			.build());
		// A value with no marker to bind it to is the same complaint from the other side.
		assertWrongNumberOfValues(
			SimpleStatement.newInstance("INSERT INTO vals.vals (pk, ck) VALUES (6, 1)", 9));

		assertEquals(0, session.execute("SELECT v FROM vals.vals WHERE pk = 6").all().size(),
			"a refused statement should have written nothing");
	}

	@Test
	@Order(233)
	@DisplayName("A null primary key part is refused, supplied as a value or written as a literal")
	void testNullPrimaryKey() {
		createValueTable();

		final var insert = "INSERT INTO vals.vals (pk, ck, v) VALUES (?, ?, ?)";
		assertNullKey("pk", SimpleStatement.newInstance(insert, null, 1, "x"));
		assertNullKey("ck", SimpleStatement.newInstance(insert, 7, null, "x"));
		assertNullKey("pk", SimpleStatement.newInstance(
			"SELECT v FROM vals.vals WHERE pk = ? AND ck = ?", null, 1));
		assertNullKey("pk", SimpleStatement.newInstance(
			"UPDATE vals.vals SET v = ? WHERE pk = ? AND ck = ?", "x", null, 1));
		assertNullKey("pk", SimpleStatement.newInstance(
			"DELETE FROM vals.vals WHERE pk = ? AND ck = ?", null, 1));

		// The same rule reaches a null written into the statement, which needs no values at all.
		assertNullKey("pk",
			SimpleStatement.newInstance("INSERT INTO vals.vals (pk, ck, v) VALUES (null, 1, 'x')"));
		assertNullKey("ck",
			SimpleStatement.newInstance("INSERT INTO vals.vals (pk, ck, v) VALUES (7, null, 'x')"));
		assertNullKey("pk", SimpleStatement.newInstance("SELECT v FROM vals.vals WHERE pk = null"));

		assertEquals(0, session.execute("SELECT v FROM vals.vals WHERE pk = 7").all().size(),
			"a refused statement should have written nothing");
	}

	@Test
	@Order(234)
	@DisplayName("A value the marker's column cannot hold is refused rather than stored")
	void testValueTypeMismatch() {
		createValueTable();

		// Only the type is asserted: a node reports the length of the bytes it was sent, and there are
		// no bytes in process, so the wording cannot be the same on both.
		final var insert = "INSERT INTO vals.vals (pk, ck, v) VALUES (?, ?, ?)";
		assertThrows(InvalidQueryException.class,
			() -> session.execute(SimpleStatement.newInstance(insert, "not an int", 1, "x")));
		assertThrows(InvalidQueryException.class,
			() -> session.execute(SimpleStatement.newInstance(insert, 8, 1L, "x")));
		assertThrows(InvalidQueryException.class, () -> session.execute(SimpleStatement.newInstance(
			"SELECT v FROM vals.vals WHERE pk = ? AND ck = ?", 8, "not an int")));

		assertEquals(0, session.execute("SELECT v FROM vals.vals WHERE pk = 8").all().size(),
			"a refused statement should have written nothing");
	}

	@Test
	@Order(235)
	@DisplayName("A named marker is addressed by the name it was written with, not by its column")
	void testNamedMarkerNames() {
		createValueTable();

		final var named = "INSERT INTO vals.vals (pk, ck, v) VALUES (:a, :b, :c)";
		session.execute(SimpleStatement.builder(named)
			.addNamedValue("a", 10)
			.addNamedValue("b", 1)
			.addNamedValue("c", "by marker")
			.build());
		assertEquals(List.of("by marker"), texts("SELECT v FROM vals.vals WHERE pk = 10"));

		// The column the marker binds is not a name the statement carries, so a marker is left over.
		assertWrongNumberOfValues(SimpleStatement.builder(named)
			.addNamedValue("pk", 11)
			.addNamedValue("ck", 1)
			.addNamedValue("v", "by column")
			.build());

		// A statement may mix the two forms, and each marker keeps its own name.
		final var mixed = "INSERT INTO vals.vals (pk, ck, v) VALUES (:a, ?, :c)";
		session.execute(SimpleStatement.builder(mixed)
			.addNamedValue("a", 12)
			.addNamedValue("ck", 1)
			.addNamedValue("c", "mixed")
			.build());
		assertEquals(List.of("mixed"), texts("SELECT v FROM vals.vals WHERE pk = 12"));
		assertWrongNumberOfValues(SimpleStatement.builder(mixed)
			.addNamedValue("a", 13)
			.addNamedValue("b", 1)
			.addNamedValue("c", "mixed")
			.build());

		// A marker whose name is another column's is still addressed by the name, not by that column.
		session.execute(SimpleStatement.builder(
				"INSERT INTO vals.vals (pk, ck, v) VALUES (:ck, :pk, :v)")
			.addNamedValue("ck", 14)
			.addNamedValue("pk", 1)
			.addNamedValue("v", "crossed")
			.build());
		assertEquals(List.of("crossed"), texts("SELECT v FROM vals.vals WHERE pk = 14 AND ck = 1"));

		final var selected = session.execute(
			SimpleStatement.builder("SELECT v FROM vals.vals WHERE pk = :x AND ck = :y")
				.addNamedValue("x", 10)
				.addNamedValue("y", 1)
				.build()).one();
		assertNotNull(selected);
		assertEquals("by marker", selected.getString(0));
	}

	@Test
	@Order(236)
	@DisplayName("One named value feeds every marker of that name, and a spare name is ignored")
	void testRepeatedAndSpareNames() {
		createValueTable();

		// Nothing is deduplicated: :x is two markers, and the one value supplied reaches both.
		session.execute(SimpleStatement.builder(
				"INSERT INTO vals.vals (pk, ck, v) VALUES (:x, :x, 'twice')")
			.addNamedValue("x", 15)
			.build());
		assertEquals(List.of("twice"), texts("SELECT v FROM vals.vals WHERE pk = 15 AND ck = 15"));

		// A node resolves named values with one lookup per marker, so a name no marker claims is not
		// a value too many - it is simply never looked at.
		session.execute(SimpleStatement.builder(
				"INSERT INTO vals.vals (pk, ck, v) VALUES (:a, :b, :c)")
			.addNamedValue("a", 16)
			.addNamedValue("b", 1)
			.addNamedValue("c", "spare")
			.addNamedValue("nosuch", "ignored")
			.build());
		assertEquals(List.of("spare"), texts("SELECT v FROM vals.vals WHERE pk = 16"));
	}

	@Test
	@Order(237)
	@DisplayName("Variable definitions are named after the markers, a ? after the column it binds")
	void testVariableNames() {
		createValueTable();

		assertVariableNames(List.of("a", "b", "c"),
			"INSERT INTO vals.vals (pk, ck, v) VALUES (:a, :b, :c)");
		assertVariableNames(List.of("pk", "ck", "v"),
			"INSERT INTO vals.vals (pk, ck, v) VALUES (?, ?, ?)");
		assertVariableNames(List.of("a", "ck", "c"),
			"INSERT INTO vals.vals (pk, ck, v) VALUES (:a, ?, :c)");
		assertVariableNames(List.of("ck", "pk", "v"),
			"INSERT INTO vals.vals (pk, ck, v) VALUES (:ck, :pk, :v)");
		// An unquoted marker name is folded to lower case, exactly as an unquoted identifier is.
		assertVariableNames(List.of("a", "b", "c"),
			"INSERT INTO vals.vals (pk, ck, v) VALUES (:A, :B, :C)");
		// Two markers of one name are two variables, not one.
		assertVariableNames(List.of("x", "x"),
			"INSERT INTO vals.vals (pk, ck, v) VALUES (:x, :x, 'twice')");
		assertVariableNames(List.of("p", "q"), "SELECT v FROM vals.vals WHERE pk = :p AND ck = :q");
		assertVariableNames(List.of("v", "p", "q"),
			"UPDATE vals.vals SET v = :v WHERE pk = :p AND ck = :q");

		// The column still types the variable, and is still what the partition key indices name.
		final var prepared = session.prepare(
			"INSERT INTO vals.vals (pk, ck, v) VALUES (:a, :b, :c)");
		final var variables = prepared.getVariableDefinitions();
		assertEquals(DataTypes.INT, variables.get(0).getType());
		assertEquals(DataTypes.TEXT, variables.get(2).getType());
		assertEquals(List.of(0), prepared.getPartitionKeyIndices());

		assertEquals(0, variables.firstIndexOf("a"));
		assertEquals(-1, variables.firstIndexOf("pk"),
			"a named marker is not addressable by the column it binds");
	}

	// The markers a statement carries outside its columns: a USING clause, an IF clause and a
	// multi-column relation. A node names and types every one of them, so they are variables like any
	// other, and a statement whose metadata leaves one out cannot be bound.

	private void createMarkerTable() {
		session.execute("CREATE TABLE IF NOT EXISTS vals.marks "
			+ "(pk int, ck int, v text, w text, PRIMARY KEY (pk, ck))");
	}

	@Test
	@Order(238)
	@DisplayName("A USING TTL or TIMESTAMP marker is a variable, named [ttl] or [timestamp]")
	void testUsingClauseVariables() {
		createMarkerTable();

		assertVariableNames(List.of("pk", "ck", "v", "[ttl]"),
			"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TTL ?");
		assertVariableNames(List.of("pk", "ck", "v", "[timestamp]"),
			"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?");
		assertVariableNames(List.of("pk", "ck", "v", "[ttl]", "[timestamp]"),
			"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TTL ? AND TIMESTAMP ?");
		assertVariableNames(List.of("pk", "ck", "v", "[timestamp]", "[ttl]"),
			"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL ?");
		// A bind index follows the text rather than the clause, so a USING clause written ahead of the
		// SET and WHERE parts is bound ahead of them.
		assertVariableNames(List.of("[ttl]", "v", "pk", "ck"),
			"UPDATE vals.marks USING TTL ? SET v = ? WHERE pk = ? AND ck = ?");
		assertVariableNames(List.of("[timestamp]", "pk", "ck"),
			"DELETE FROM vals.marks USING TIMESTAMP ? WHERE pk = ? AND ck = ?");
		// The naming rule reaches these markers too: one written with a name keeps it.
		assertVariableNames(List.of("pk", "ck", "v", "ttl"),
			"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TTL :ttl");
		assertVariableNames(List.of("ts", "pk", "ck"),
			"DELETE FROM vals.marks USING TIMESTAMP :ts WHERE pk = ? AND ck = ?");

		// A TTL is an int and a timestamp a bigint; neither is the type of any column.
		final var prepared = session.prepare(
			"UPDATE vals.marks USING TTL ? AND TIMESTAMP ? SET v = ? WHERE pk = ? AND ck = ?");
		final var variables = prepared.getVariableDefinitions();
		assertEquals(DataTypes.INT, variables.get(0).getType());
		assertEquals(DataTypes.BIGINT, variables.get(1).getType());
		// The partition key index still names the marker binding the column, which the two markers
		// ahead of it have pushed along.
		assertEquals(List.of(3), prepared.getPartitionKeyIndices());
	}

	@Test
	@Order(239)
	@DisplayName("An IF condition's marker and a multi-column relation's are variables of their column")
	void testConditionAndTupleVariables() {
		createMarkerTable();

		assertVariableNames(List.of("v", "pk", "ck", "v"),
			"UPDATE vals.marks SET v = ? WHERE pk = ? AND ck = ? IF v = ?");
		assertVariableNames(List.of("v", "pk", "ck", "cond"),
			"UPDATE vals.marks SET v = ? WHERE pk = ? AND ck = ? IF v = :cond");
		assertVariableNames(List.of("v", "pk", "ck", "v", "w"),
			"UPDATE vals.marks SET v = ? WHERE pk = ? AND ck = ? IF v = ? AND w = ?");
		// IF NOT EXISTS and IF EXISTS carry no marker of their own.
		assertVariableNames(List.of("pk", "ck", "v"),
			"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS");
		assertVariableNames(List.of("v", "pk", "ck"),
			"UPDATE vals.marks SET v = ? WHERE pk = ? AND ck = ? IF EXISTS");
		// A USING clause and an IF clause bracket the statement, one bound first and one bound last.
		assertVariableNames(List.of("[ttl]", "v", "pk", "ck", "v"),
			"UPDATE vals.marks USING TTL ? SET v = ? WHERE pk = ? AND ck = ? IF v = ?");

		// A multi-column relation's markers sit inside a tuple, and are typed one per column.
		assertVariableNames(List.of("pk", "ck"),
			"SELECT v FROM vals.marks WHERE pk = ? AND (ck) = (?)");
		assertVariableNames(List.of("pk", "ck", "ck"),
			"SELECT v FROM vals.marks WHERE pk = ? AND (ck) IN ((?), (?))");

		final var prepared = session.prepare(
			"UPDATE vals.marks SET v = ? WHERE pk = ? AND ck = ? IF w = ?");
		final var variables = prepared.getVariableDefinitions();
		assertEquals(DataTypes.TEXT, variables.get(3).getType());
		assertEquals(List.of(1), prepared.getPartitionKeyIndices());
	}

	@Test
	@Order(240)
	@DisplayName("A bound TTL, timestamp, IF condition and tuple value each take effect")
	void testBoundMarkerValues() {
		createMarkerTable();

		session.execute(session.prepare(
				"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TTL ?")
			.bind(20, 1, "ttl", 3600));
		final var ttl = only("SELECT ttl(v) FROM vals.marks WHERE pk = 20 AND ck = 1").getInt(0);
		assertTrue(ttl > 3500 && ttl <= 3600, "ttl should be counting down from 3600 but was " + ttl);

		session.execute(session.prepare(
				"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?")
			.bind(21, 1, "ts", 4000L));
		assertEquals(4000L, only("SELECT writetime(v) FROM vals.marks WHERE pk = 21").getLong(0));

		// The UPDATE form, where the marker is written ahead of every other one in the statement.
		session.execute(session.prepare(
				"UPDATE vals.marks USING TIMESTAMP ? SET v = ? WHERE pk = ? AND ck = ?")
			.bind(9000L, "later", 21, 1));
		assertEquals(9000L, only("SELECT writetime(v) FROM vals.marks WHERE pk = 21").getLong(0));
		assertEquals(List.of("later"), texts("SELECT v FROM vals.marks WHERE pk = 21"));

		// A bound IF condition decides whether the write applies.
		session.execute("INSERT INTO vals.marks (pk, ck, v, w) VALUES (22, 1, 'old', 'guard')");
		final var conditional = session.prepare(
			"UPDATE vals.marks SET v = ? WHERE pk = ? AND ck = ? IF w = ?");
		final var refused = session.execute(conditional.bind("new", 22, 1, "wrong")).one();
		assertNotNull(refused);
		assertFalse(refused.getBoolean("[applied]"), "a condition on the wrong value should not apply");
		assertEquals(List.of("old"), texts("SELECT v FROM vals.marks WHERE pk = 22"));

		final var accepted = session.execute(conditional.bind("new", 22, 1, "guard")).one();
		assertNotNull(accepted);
		assertTrue(accepted.getBoolean("[applied]"), "a condition on the stored value should apply");
		assertEquals(List.of("new"), texts("SELECT v FROM vals.marks WHERE pk = 22"));

		// A bound multi-column relation value.
		session.execute("INSERT INTO vals.marks (pk, ck, v) VALUES (23, 1, 'tuple')");
		final var tuple = session.execute(
			session.prepare("SELECT v FROM vals.marks WHERE pk = ? AND (ck) = (?)").bind(23, 1)).one();
		assertNotNull(tuple);
		assertEquals("tuple", tuple.getString(0));

		// The same markers reached from a statement carrying its own values rather than a prepared one.
		session.execute(SimpleStatement.newInstance(
			"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", 24, 1, "simple",
			5000L));
		assertEquals(5000L, only("SELECT writetime(v) FROM vals.marks WHERE pk = 24").getLong(0));
		session.execute(SimpleStatement.builder(
				"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP :ts")
			.addNamedValue("pk", 25)
			.addNamedValue("ck", 1)
			.addNamedValue("v", "named")
			.addNamedValue("ts", 6000L)
			.build());
		assertEquals(6000L, only("SELECT writetime(v) FROM vals.marks WHERE pk = 25").getLong(0));
		// An anonymous USING marker is named [ttl], which no named value can be addressed to, so the
		// columns alone leave a marker unaccounted for.
		assertWrongNumberOfValues(SimpleStatement.builder(
				"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TTL ?")
			.addNamedValue("pk", 26)
			.addNamedValue("ck", 1)
			.addNamedValue("v", "short")
			.build());
		assertWrongNumberOfValues(SimpleStatement.newInstance(
			"INSERT INTO vals.marks (pk, ck, v) VALUES (?, ?, ?) USING TTL ?", 26, 1, "short"));
	}

	private void createElementTable() {
		session.execute("CREATE TYPE IF NOT EXISTS vals.mark_addr (street text)");
		session.execute("CREATE TABLE IF NOT EXISTS vals.elems (pk int PRIMARY KEY, l list<text>, "
			+ "s set<text>, m map<text, text>, u mark_addr)");
	}

	@Test
	@Order(241)
	@DisplayName("A marker addressing one element or field is typed by what it addresses")
	void testElementAndFieldVariables() {
		createElementTable();

		// A selector is not the column: a list index is an int, a map key is the key type.
		assertVariableNames(List.of("idx(l)", "pk"), "DELETE l[?] FROM vals.elems WHERE pk = ?");
		assertVariableNames(List.of("value(s)", "pk"), "DELETE s[?] FROM vals.elems WHERE pk = ?");
		assertVariableNames(List.of("key(m)", "pk"), "DELETE m[?] FROM vals.elems WHERE pk = ?");
		assertVariableNames(List.of("idx(l)", "value(l)", "pk"),
			"UPDATE vals.elems SET l[?] = ? WHERE pk = ?");
		assertVariableNames(List.of("key(m)", "value(m)", "pk"),
			"UPDATE vals.elems SET m[?] = ? WHERE pk = ?");
		assertVariableNames(List.of("u.street", "pk"),
			"UPDATE vals.elems SET u.street = ? WHERE pk = ?");
		assertVariableNames(List.of("value(m)", "pk", "value(m)"),
			"UPDATE vals.elems SET m['a'] = ? WHERE pk = ? IF m['b'] = ?");
		assertVariableNames(List.of("l", "pk"), "UPDATE vals.elems SET l = l + ? WHERE pk = ?");

		final var prepared = session.prepare("UPDATE vals.elems SET m[?] = ? WHERE pk = ?");
		final var variables = prepared.getVariableDefinitions();
		assertEquals(DataTypes.TEXT, variables.get(0).getType());
		assertEquals(DataTypes.TEXT, variables.get(1).getType());
		assertEquals(DataTypes.INT,
			session.prepare("DELETE l[?] FROM vals.elems WHERE pk = ?").getVariableDefinitions()
				.get(0).getType());
	}

	@Test
	@Order(242)
	@DisplayName("An INSERT ... JSON document is a single text variable, named [json]")
	void testJsonVariables() {
		createElementTable();

		assertVariableNames(List.of("[json]"), "INSERT INTO vals.elems JSON ?");
		assertVariableNames(List.of("doc"), "INSERT INTO vals.elems JSON :doc");
		assertVariableNames(List.of("[json]"), "INSERT INTO vals.elems JSON ? DEFAULT UNSET");
		assertVariableNames(List.of("[json]", "[ttl]"),
			"INSERT INTO vals.elems JSON ? USING TTL ?");
		assertEquals(DataTypes.TEXT,
			session.prepare("INSERT INTO vals.elems JSON ?").getVariableDefinitions().get(0).getType());

		session.execute(session.prepare("INSERT INTO vals.elems JSON ?")
			.bind("{\"pk\": 30, \"m\": {\"a\": \"x\", \"b\": \"y\"}}"));
		final var stored = only("SELECT m FROM vals.elems WHERE pk = 30");
		assertEquals(Map.of("a", "x", "b", "y"), stored.getMap(0, String.class, String.class));

		// The selector and the value are bound separately, and to the narrower type each addresses.
		session.execute(session.prepare("UPDATE vals.elems SET m[?] = ? WHERE pk = ?")
			.bind("a", "z", 30));
		session.execute(session.prepare("DELETE m[?] FROM vals.elems WHERE pk = ?").bind("b", 30));
		assertEquals(Map.of("a", "z"),
			only("SELECT m FROM vals.elems WHERE pk = 30").getMap(0, String.class, String.class));

		session.execute("UPDATE vals.elems SET u = {street: 'Old'} WHERE pk = 30");
		session.execute(session.prepare("UPDATE vals.elems SET u.street = ? WHERE pk = ?")
			.bind("Main", 30));
		final var udt = only("SELECT u FROM vals.elems WHERE pk = 30").getUdtValue(0);
		assertNotNull(udt);
		assertEquals("Main", udt.getString("street"));
	}

	private void assertVariableNames(final List<String> expected, final String cql) {
		final var variables = session.prepare(cql).getVariableDefinitions();
		final var names = StreamSupport.stream(variables.spliterator(), false)
			.map(definition -> definition.getName().asInternal())
			.toList();
		assertEquals(expected, names, cql);
	}

	private void assertWrongNumberOfValues(final SimpleStatement statement) {
		assertMentions("Invalid amount of bind variables",
			assertThrows(InvalidQueryException.class, () -> session.execute(statement),
				"expected to be rejected: " + statement.getQuery()));
	}

	/**
	 * Asserts a statement is refused for a null in the primary key. Cassandra reads an INSERT's key
	 * columns as conditions the way it reads a WHERE clause, so both report the same wording.
	 */
	private void assertNullKey(final String column, final SimpleStatement statement) {
		assertMentions("Invalid null value in condition for column " + column,
			assertThrows(InvalidQueryException.class, () -> session.execute(statement),
				"expected to be rejected: " + statement.getQuery()));
	}

}
