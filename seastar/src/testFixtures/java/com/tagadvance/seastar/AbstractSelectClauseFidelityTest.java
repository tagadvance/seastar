package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * The select clause: aggregates, the per-cell functions, casts, aliases, {@code DISTINCT} and the
 * JSON forms in both directions. This group owns the {@code sel} keyspace.
 */
public abstract class AbstractSelectClauseFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("sel");
	}

	private void createDistinctTable() {
		session.execute("CREATE TABLE IF NOT EXISTS sel.distinct_test "
			+ "(pk int, ck int, val text, PRIMARY KEY (pk, ck))");
		session.execute("INSERT INTO sel.distinct_test (pk, ck, val) VALUES (1, 1, 'a')");
		session.execute("INSERT INTO sel.distinct_test (pk, ck, val) VALUES (1, 2, 'b')");
		session.execute("INSERT INTO sel.distinct_test (pk, ck, val) VALUES (2, 1, 'c')");
	}

	@Test
	@Order(48)
	@DisplayName("SELECT DISTINCT on the partition key returns one row per partition")
	void testSelectDistinctPartitionKey() {
		createDistinctTable();

		final var rows = session.execute("SELECT DISTINCT pk FROM sel.distinct_test").all();

		final var partitions = rows.stream().map(row -> row.getInt("pk")).collect(Collectors.toSet());
		assertEquals(Set.of(1, 2), partitions);
	}

	@Test
	@Order(49)
	@DisplayName("SELECT DISTINCT on a non-partition-key column throws InvalidQueryException")
	void testSelectDistinctNonPartitionKey() {
		createDistinctTable();

		assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT DISTINCT val FROM sel.distinct_test"));
	}

	/**
	 * A table with two partitions and a null in one cell, which is what tells an aggregate that skips
	 * nulls from one that does not.
	 */
	private void createAggregateTable() {
		session.execute("CREATE TABLE IF NOT EXISTS sel.agg "
			+ "(pk int, ck int, n int, v text, PRIMARY KEY (pk, ck))");
		Stream.of("(1, 1, 10, 'a')", "(1, 2, 20, null)", "(2, 1, 5, 'c')")
			.forEach(values -> session.execute(
				"INSERT INTO sel.agg (pk, ck, n, v) VALUES " + values));
	}

	@Test
	@Order(200)
	@DisplayName("count(*) counts rows and count(col) counts the rows where the column is not null")
	void testCount() {
		createAggregateTable();

		assertEquals(3L, only("SELECT count(*) FROM sel.agg").getLong(0));
		assertEquals(2L, only("SELECT count(v) FROM sel.agg").getLong(0));
		assertEquals(3L, only("SELECT count(n) FROM sel.agg").getLong(0));
		assertEquals(2L, only("SELECT count(*) FROM sel.agg WHERE pk = 1").getLong(0));

		assertEquals(List.of("count", "system.count(v)"),
			columnNames("SELECT count(*), count(v) FROM sel.agg"));
	}

	@Test
	@Order(201)
	@DisplayName("min, max, sum and avg fold in the column's own type")
	void testAggregates() {
		createAggregateTable();

		final var numbers = only("SELECT min(n), max(n), sum(n), avg(n) FROM sel.agg");
		assertEquals(5, numbers.getInt(0));
		assertEquals(20, numbers.getInt(1));
		assertEquals(35, numbers.getInt(2));
		// An int column averages to an int, so 35 / 3 is 11 rather than 11.67.
		assertEquals(11, numbers.getInt(3));

		final var texts = only("SELECT min(v), max(v) FROM sel.agg");
		assertEquals("a", texts.getString(0));
		assertEquals("c", texts.getString(1));
	}

	@Test
	@Order(202)
	@DisplayName("An aggregate over nothing answers zero for count and sum, and null for min and max")
	void testAggregatesOverEmptyResult() {
		session.execute("CREATE TABLE IF NOT EXISTS sel.aggempty (pk int PRIMARY KEY, n int, v text)");

		assertEquals(0L, only("SELECT count(*) FROM sel.aggempty").getLong(0));
		assertEquals(0L, only("SELECT count(n) FROM sel.aggempty").getLong(0));

		final var row = only("SELECT min(n), max(n), sum(n), avg(n), min(v) FROM sel.aggempty");
		assertTrue(row.isNull(0), "min over an empty result should be null");
		assertTrue(row.isNull(1), "max over an empty result should be null");
		assertEquals(0, row.getInt(2), "sum over an empty result should be zero");
		assertEquals(0, row.getInt(3), "avg over an empty result should be zero");
		assertTrue(row.isNull(4), "min over an empty result should be null");
	}

	@Test
	@Order(203)
	@DisplayName("sum needs a numeric column, and an unknown function is named")
	void testAggregateTypeErrors() {
		createAggregateTable();

		assertInvalid("SELECT sum(v) FROM sel.agg", "sum");
		assertInvalid("SELECT avg(v) FROM sel.agg", "avg");
		assertInvalid("SELECT count(nosuch) FROM sel.agg", "nosuch");
		assertInvalid("SELECT nosuchfunction(n) FROM sel.agg", "nosuchfunction");
	}

	@Test
	@Order(204)
	@DisplayName("An alias names the result column, and a plain column beside an aggregate is kept")
	void testAliases() {
		createAggregateTable();

		assertEquals(List.of("c", "value"),
			columnNames("SELECT ck AS c, v AS value FROM sel.agg WHERE pk = 1"));
		assertEquals(List.of("total"), columnNames("SELECT count(*) AS total FROM sel.agg"));
		assertEquals(3L, only("SELECT count(*) AS total FROM sel.agg").getLong("total"));

		// A cluster answers a plain column beside an aggregate with the first matched row's value.
		final var mixed = only("SELECT count(*), pk FROM sel.agg WHERE pk = 1");
		assertEquals(2L, mixed.getLong(0));
		assertEquals(1, mixed.getInt(1));
	}

	@Test
	@Order(205)
	@DisplayName("token(pk) answers the Murmur3 token a row is stored at")
	void testToken() {
		createAggregateTable();

		assertEquals(List.of("system.token(pk)"), columnNames("SELECT token(pk) FROM sel.agg"));
		assertEquals(-4069959284402364209L,
			only("SELECT token(pk) FROM sel.agg WHERE pk = 1").getLong(0));
		// token() is defined over the partition key's types rather than its columns, so a call whose
		// argument types do not line up is what gets refused.
		assertInvalid("SELECT token(v) FROM sel.agg", "token");
		assertInvalid("SELECT token(pk, ck) FROM sel.agg", "token");
	}

	@Test
	@Order(206)
	@DisplayName("cast converts between the numeric types and to text")
	void testCast() {
		createAggregateTable();

		final var row = only("SELECT cast(n AS text), cast(n AS double) FROM sel.agg WHERE pk = 2");
		assertEquals("5", row.getString(0));
		assertEquals(5.0, row.getDouble(1));
		assertEquals(List.of("cast(n as text)"), columnNames("SELECT cast(n AS text) FROM sel.agg"));
	}

	@Test
	@Order(207)
	@DisplayName("SELECT JSON returns one [json] text column holding every selected column")
	void testSelectJson() {
		session.execute("CREATE TABLE IF NOT EXISTS sel.js (pk int PRIMARY KEY, a text, b int)");
		session.execute("INSERT INTO sel.js (pk, a, b) VALUES (1, 'x', 2)");
		session.execute("INSERT INTO sel.js (pk) VALUES (9)");

		assertEquals(List.of("[json]"), columnNames("SELECT JSON * FROM sel.js"));
		assertEquals("{\"pk\": 1, \"a\": \"x\", \"b\": 2}",
			only("SELECT JSON * FROM sel.js WHERE pk = 1").getString(0));
		assertEquals("{\"pk\": 9, \"a\": null, \"b\": null}",
			only("SELECT JSON * FROM sel.js WHERE pk = 9").getString(0));
		assertEquals("{\"b\": 2, \"a\": \"x\"}",
			only("SELECT JSON b, a FROM sel.js WHERE pk = 1").getString(0));
		assertEquals("{\"count\": 2}", only("SELECT JSON count(*) FROM sel.js").getString(0));
		assertEquals("{\"total\": 2}",
			only("SELECT JSON count(*) AS total FROM sel.js").getString(0));
	}

	@Test
	@Order(208)
	@DisplayName("INSERT JSON writes the columns the document names and nulls the rest")
	void testInsertJson() {
		session.execute("CREATE TABLE IF NOT EXISTS sel.insjson "
			+ "(pk int PRIMARY KEY, a text, b int, l list<int>, m map<text, int>)");
		session.execute("INSERT INTO sel.insjson (pk, a, b) VALUES (1, 'x', 2)");

		// DEFAULT NULL is the default: a column the document leaves out is cleared.
		session.execute("INSERT INTO sel.insjson JSON '{\"pk\": 1, \"a\": \"y\"}'");
		final var cleared = only("SELECT a, b FROM sel.insjson WHERE pk = 1");
		assertEquals("y", cleared.getString(0));
		assertTrue(cleared.isNull(1));

		// DEFAULT UNSET leaves it alone instead.
		session.execute("INSERT INTO sel.insjson JSON '{\"pk\": 1, \"b\": 9}' DEFAULT UNSET");
		final var kept = only("SELECT a, b FROM sel.insjson WHERE pk = 1");
		assertEquals("y", kept.getString(0));
		assertEquals(9, kept.getInt(1));

		session.execute(
			"INSERT INTO sel.insjson JSON '{\"pk\": 2, \"l\": [1, 2], \"m\": {\"k\": 7}}'");
		final var collections = only("SELECT l, m FROM sel.insjson WHERE pk = 2");
		assertEquals(List.of(1, 2), collections.getList(0, Integer.class));
		assertEquals(Map.of("k", 7), collections.getMap(1, String.class, Integer.class));

		assertInvalid("INSERT INTO sel.insjson JSON '{\"pk\": 3, \"nosuch\": 1}'", "nosuch");
		assertInvalid("INSERT INTO sel.insjson JSON '{\"pk\": 3, \"b\": \"nope\"}'", "b");
		assertThrows(InvalidQueryException.class,
			() -> session.execute("INSERT INTO sel.insjson JSON 'not json'"));
	}

	@Test
	@Order(209)
	@DisplayName("A JSON round trip preserves every column of a row")
	void testJsonRoundTrip() {
		session.execute("CREATE TABLE IF NOT EXISTS sel.jsround (pk int PRIMARY KEY, t timestamp, "
			+ "u uuid, b blob, d decimal, f double, s set<text>, bo boolean, i inet, da date)");
		session.execute("INSERT INTO sel.jsround (pk, t, u, b, d, f, s, bo, i, da) VALUES "
			+ "(1, 1700000000000, 123e4567-e89b-12d3-a456-426614174000, 0x00ff, 1.25, 2.5, "
			+ "{'a', 'b'}, true, '127.0.0.1', '2024-01-15')");

		final var json = only("SELECT JSON * FROM sel.jsround WHERE pk = 1").getString(0);
		session.execute("INSERT INTO sel.jsround JSON '" + json.replace("\"pk\": 1", "\"pk\": 2")
			+ "'");

		final var copy = only("SELECT JSON * FROM sel.jsround WHERE pk = 2").getString(0);
		assertEquals(json.replace("\"pk\": 1", "\"pk\": 2"), copy);
	}

}
