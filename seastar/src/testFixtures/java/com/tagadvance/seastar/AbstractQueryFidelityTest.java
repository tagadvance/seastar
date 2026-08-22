package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * The query engine: the order rows come back in, WHERE restrictions and the rules about which of
 * them a table will answer, the UPDATE assignment forms and counters. This group owns the
 * {@code query} keyspace.
 */
public abstract class AbstractQueryFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("query");
	}

	private List<Integer> clustering(final String cql) {
		return session.execute(cql).all().stream().map(row -> row.getInt(0)).toList();
	}

	private List<Integer> ints(final String cql) {
		return session.execute(cql).all().stream().map(row -> row.getInt(0)).sorted().toList();
	}

	@Test
	@Order(110)
	@DisplayName("A partition reads back in clustering order, and LIMIT applies to that order")
	void testClusteringOrder() {
		session.execute("CREATE TABLE IF NOT EXISTS query.evt_order (pk int, ck int, v text, "
			+ "PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
		session.execute("INSERT INTO query.evt_order (pk, ck, v) VALUES (1, 2, 'b')");
		session.execute("INSERT INTO query.evt_order (pk, ck, v) VALUES (1, 1, 'a')");
		session.execute("INSERT INTO query.evt_order (pk, ck, v) VALUES (1, 3, 'c')");

		assertEquals(List.of(3, 2, 1), clustering("SELECT ck FROM query.evt_order WHERE pk = 1"));
		// The limit takes the first rows of the ordered partition, not the first ones written.
		assertEquals(List.of(3, 2), clustering("SELECT ck FROM query.evt_order WHERE pk = 1 LIMIT 2"));
	}

	@Test
	@Order(111)
	@DisplayName("Clustering columns are compared in declaration order, each in its own direction")
	void testMixedClusteringOrder() {
		session.execute("CREATE TABLE IF NOT EXISTS query.two (pk int, c1 int, c2 int, "
			+ "PRIMARY KEY (pk, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");
		for (final var c1 : List.of(2, 1)) {
			for (final var c2 : List.of(1, 2)) {
				session.execute(
					"INSERT INTO query.two (pk, c1, c2) VALUES (1, %d, %d)".formatted(c1, c2));
			}
		}

		assertEquals(List.of("1/2", "1/1", "2/2", "2/1"),
			session.execute("SELECT c1, c2 FROM query.two WHERE pk = 1")
				.all()
				.stream()
				.map(row -> row.getInt("c1") + "/" + row.getInt("c2"))
				.toList());
	}

	@Test
	@Order(112)
	@DisplayName("Partitions come back in Murmur3 token order, never in the order they were written")
	void testPartitionOrder() {
		session.execute("CREATE TABLE IF NOT EXISTS query.parts (pk int PRIMARY KEY, v text)");
		for (int pk = 1; pk <= 10; pk++) {
			session.execute("INSERT INTO query.parts (pk, v) VALUES (%d, 'v%d')".formatted(pk, pk));
		}

		// The Murmur3 token order of the integers 1..10, read off a real node.
		final var tokenOrder = List.of(5, 10, 1, 8, 2, 4, 7, 6, 9, 3);
		assertEquals(tokenOrder, clustering("SELECT pk FROM query.parts"));
		assertEquals(tokenOrder, clustering("SELECT DISTINCT pk FROM query.parts"));
	}

	@Test
	@Order(113)
	@DisplayName("ORDER BY reads the clustering order forwards or backwards")
	void testOrderBy() {
		session.execute("CREATE TABLE IF NOT EXISTS query.ordered (pk int, ck int, "
			+ "PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
		Stream.of(2, 1, 3)
			.forEach(ck -> session.execute(
				"INSERT INTO query.ordered (pk, ck) VALUES (1, %d)".formatted(ck)));

		assertEquals(List.of(3, 2, 1),
			clustering("SELECT ck FROM query.ordered WHERE pk = 1 ORDER BY ck DESC"));
		assertEquals(List.of(1, 2, 3),
			clustering("SELECT ck FROM query.ordered WHERE pk = 1 ORDER BY ck ASC"));
		// The limit applies to the ordering asked for, not to the one the table declares.
		assertEquals(List.of(1, 2),
			clustering("SELECT ck FROM query.ordered WHERE pk = 1 ORDER BY ck ASC LIMIT 2"));
	}

	@Test
	@Order(114)
	@DisplayName("ORDER BY is rejected off a single partition, off the clustering key, or mixed")
	void testOrderByIsRejected() {
		session.execute("CREATE TABLE IF NOT EXISTS query.reject (pk int, c1 int, c2 int, v text, "
			+ "PRIMARY KEY (pk, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");
		session.execute("INSERT INTO query.reject (pk, c1, c2, v) VALUES (1, 1, 1, 'a')");
		session.execute("INSERT INTO query.reject (pk, c1, c2, v) VALUES (2, 1, 1, 'b')");

		// The partition key has to be pinned, because ORDER BY reads one partition.
		assertMentions("partition key",
			assertThrows(InvalidQueryException.class,
				() -> session.execute("SELECT * FROM query.reject ORDER BY c1")));
		assertMentions("partition key",
			assertThrows(InvalidQueryException.class, () -> session.execute(
				"SELECT * FROM query.reject WHERE v = 'a' ORDER BY c1 ALLOW FILTERING")));
		// An IN reads several partitions, which a paged query cannot merge.
		assertMentions("page",
			assertThrows(InvalidQueryException.class,
				() -> session.execute("SELECT * FROM query.reject WHERE pk IN (1, 2) ORDER BY c1")));
		// Only clustering columns, in their declared order.
		assertMentions("v", assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT * FROM query.reject WHERE pk = 1 ORDER BY v")));
		assertMentions("pk", assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT * FROM query.reject WHERE pk = 1 ORDER BY pk")));
		assertMentions("declared order", assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT * FROM query.reject WHERE pk = 1 ORDER BY c2 DESC")));
		assertMentions("nope", assertThrows(InvalidQueryException.class,
			() -> session.execute("SELECT * FROM query.reject WHERE pk = 1 ORDER BY nope")));
		// Every element has to agree on reading the declared order forwards or backwards.
		assertMentions("unsupported", assertThrows(InvalidQueryException.class, () -> session.execute(
			"SELECT * FROM query.reject WHERE pk = 1 ORDER BY c1 ASC, c2 ASC")));

		// A clustering column pinned by an EQ may be skipped over.
		assertDoesNotThrow(() -> session.execute(
			"SELECT * FROM query.reject WHERE pk = 1 AND c1 = 1 ORDER BY c2 ASC"));
		// Naming only the first clustering column reverses the whole order.
		assertDoesNotThrow(
			() -> session.execute("SELECT * FROM query.reject WHERE pk = 1 ORDER BY c1 DESC"));
	}

	@Test
	@Order(115)
	@DisplayName("A clustering column is ordered by its CQL type, not by Object#compareTo")
	void testClusteringOrderByType() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS query.typed (pk int, t text, PRIMARY KEY (pk, t))");
		Stream.of("b", "A", "a", "B", "\u00e9", "1")
			.forEach(t -> session.execute("INSERT INTO query.typed (pk, t) VALUES (1, '%s')".formatted(t)));
		assertEquals(List.of("1", "A", "B", "a", "b", "\u00e9"),
			session.execute("SELECT t FROM query.typed WHERE pk = 1")
				.all()
				.stream()
				.map(row -> row.getString("t"))
				.toList());

		session.execute(
			"CREATE TABLE IF NOT EXISTS query.blobs (pk int, b blob, PRIMARY KEY (pk, b))");
		Stream.of("0x01", "0xff", "0x00", "0x0100", "0x7f")
			.forEach(b -> session.execute("INSERT INTO query.blobs (pk, b) VALUES (1, %s)".formatted(b)));
		assertEquals(List.of("00", "01", "0100", "7f", "ff"),
			session.execute("SELECT b FROM query.blobs WHERE pk = 1")
				.all()
				.stream()
				.map(row -> hex(row.getByteBuffer("b")))
				.toList());

		// A uuid orders on its version and then unsigned on each half, where UUID#compareTo would
		// read the halves as signed longs and put the two large ones first.
		session.execute("CREATE TABLE IF NOT EXISTS query.uuids (pk int, u uuid, PRIMARY KEY (pk, u))");
		final var uuids = List.of("ffffffff-0000-1000-8000-000000000000",
			"00000000-0000-1000-8000-000000000000", "80000000-0000-1000-8000-000000000000",
			"7fffffff-0000-1000-8000-000000000000");
		uuids.forEach(u -> session.execute("INSERT INTO query.uuids (pk, u) VALUES (1, %s)".formatted(u)));
		assertEquals(uuids.stream().sorted().toList(),
			session.execute("SELECT u FROM query.uuids WHERE pk = 1")
				.all()
				.stream()
				.map(row -> row.getUuid("u").toString())
				.toList());
	}

	// The restriction rules, the UPDATE assignment forms and counters.

	/**
	 * A table with a two-part clustering key, which is what the restriction rules are about: a
	 * partition to reach and clustering columns to walk left to right.
	 */
	private void createEventTable() {
		session.execute("CREATE TABLE IF NOT EXISTS query.evt "
			+ "(pk int, ck1 int, ck2 int, v text, PRIMARY KEY ((pk), ck1, ck2))");
		Stream.of("(1, 1, 1, 'a')", "(1, 1, 2, 'b')", "(1, 2, 1, 'c')", "(1, 3, 1, 'd')",
				"(2, 1, 1, 'e')")
			.forEach(values -> session.execute(
				"INSERT INTO query.evt (pk, ck1, ck2, v) VALUES " + values));
	}

	private void createCollectionTable() {
		session.execute("CREATE TABLE IF NOT EXISTS query.writes (id int PRIMARY KEY, l list<int>, "
			+ "s set<int>, m map<text, int>, n text)");
	}

	@Test
	@Order(120)
	@DisplayName("A range on a clustering column selects the rows inside it")
	void testClusteringRange() {
		createEventTable();

		assertEquals(List.of("c", "d"), texts("SELECT v FROM query.evt WHERE pk = 1 AND ck1 > 1"));
		assertEquals(List.of("a", "b", "c"), texts("SELECT v FROM query.evt WHERE pk = 1 AND ck1 <= 2"));
		assertEquals(List.of("c"),
			texts("SELECT v FROM query.evt WHERE pk = 1 AND ck1 > 1 AND ck1 < 3"));
		assertEquals(List.of("b"),
			texts("SELECT v FROM query.evt WHERE pk = 1 AND ck1 = 1 AND ck2 > 1"));
	}

	@Test
	@Order(121)
	@DisplayName("A multi-column relation compares the clustering columns lexicographically")
	void testMultiColumnRelation() {
		createEventTable();

		assertEquals(List.of("b", "c", "d"),
			texts("SELECT v FROM query.evt WHERE pk = 1 AND (ck1, ck2) > (1, 1)"));
		assertEquals(List.of("a", "b"),
			texts("SELECT v FROM query.evt WHERE pk = 1 AND (ck1, ck2) < (2, 1)"));
		assertEquals(List.of("a", "c"),
			texts("SELECT v FROM query.evt WHERE pk = 1 AND (ck1, ck2) IN ((1, 1), (2, 1))"));
		// A multi-column relation names clustering columns, in key order, and nothing else.
		assertInvalid("SELECT * FROM query.evt WHERE (pk, ck1) = (1, 1)", "pk");
		assertInvalid("SELECT * FROM query.evt WHERE pk = 1 AND (ck2, ck1) > (1, 1)", "ck");
	}

	@Test
	@Order(122)
	@DisplayName("Clustering columns are restricted left to right, with a range only on the last")
	void testClusteringPrefixRules() {
		createEventTable();

		assertInvalid("SELECT * FROM query.evt WHERE pk = 1 AND ck2 = 1", "ck2");
		assertInvalid("SELECT * FROM query.evt WHERE pk = 1 AND ck1 > 1 AND ck2 > 1", "ck2");
		// ALLOW FILTERING is Cassandra's way of saying "scan", and a scan has no prefix to respect.
		assertEquals(List.of("a", "c", "d", "e"),
			texts("SELECT v FROM query.evt WHERE ck2 = 1 ALLOW FILTERING"));
		assertEquals(List.of("d"),
			texts("SELECT v FROM query.evt WHERE pk = 1 AND ck1 > 2 AND ck2 > 0 ALLOW FILTERING"));
		// IN pins a column just as = does, so a range may still follow it.
		assertEquals(List.of("b"),
			texts("SELECT v FROM query.evt WHERE pk = 1 AND ck1 IN (1, 2) AND ck2 > 1"));
	}

	@Test
	@Order(123)
	@DisplayName("A query that cannot reach one partition needs ALLOW FILTERING")
	void testFilteringRules() {
		createEventTable();
		session.execute("CREATE TABLE IF NOT EXISTS query.two_part "
			+ "(pk1 int, pk2 int, v text, PRIMARY KEY ((pk1, pk2)))");
		session.execute("INSERT INTO query.two_part (pk1, pk2, v) VALUES (1, 2, 'x')");

		Stream.of("SELECT v FROM query.evt WHERE ck1 = 1", "SELECT v FROM query.evt WHERE pk > 1",
				"SELECT v FROM query.evt WHERE pk = 1 AND v = 'a'",
				"SELECT v FROM query.two_part WHERE pk1 = 1")
			.forEach(cql -> assertInvalid(cql, "ALLOW FILTERING"));

		assertEquals(List.of("a", "b", "e"),
			texts("SELECT v FROM query.evt WHERE ck1 = 1 ALLOW FILTERING"));
		assertEquals(List.of("e"), texts("SELECT v FROM query.evt WHERE pk > 1 ALLOW FILTERING"));
		assertEquals(List.of("x"), texts("SELECT v FROM query.two_part WHERE pk1 = 1 ALLOW FILTERING"));
		// The whole partition key pinned by = or IN reaches partitions without a scan.
		assertEquals(List.of("x"), texts("SELECT v FROM query.two_part WHERE pk1 = 1 AND pk2 = 2"));
	}

	@Test
	@Order(124)
	@DisplayName("A column restricted twice, one of them by equality, is rejected")
	void testConflictingRestrictions() {
		createEventTable();

		assertInvalid("SELECT * FROM query.evt WHERE pk = 1 AND ck1 = 1 AND ck1 = 2", "ck1");
		assertInvalid("SELECT * FROM query.evt WHERE pk = 1 AND ck1 = 1 AND ck1 > 0", "ck1");
		assertInvalid("SELECT * FROM query.evt WHERE pk = 1 AND ck1 > 1 AND ck1 > 2", "ck1");
		// One lower bound and one upper bound is a range, not a conflict.
		assertEquals(List.of("c"),
			texts("SELECT v FROM query.evt WHERE pk = 1 AND ck1 > 1 AND ck1 < 3"));
	}

	@Test
	@Order(125)
	@DisplayName("CONTAINS matches a collection's elements and CONTAINS KEY a map's keys")
	void testContains() {
		createCollectionTable();
		session.execute("INSERT INTO query.writes (id, l, s, m, n) VALUES "
			+ "(1, [1, 2], {3}, {'a': 4}, 'text')");
		session.execute("INSERT INTO query.writes (id, l, s, m, n) VALUES "
			+ "(2, [5], {6}, {'b': 7}, 'more')");

		assertEquals(List.of(1), ints("SELECT id FROM query.writes WHERE l CONTAINS 2 ALLOW FILTERING"));
		assertEquals(List.of(2), ints("SELECT id FROM query.writes WHERE s CONTAINS 6 ALLOW FILTERING"));
		assertEquals(List.of(1), ints("SELECT id FROM query.writes WHERE m CONTAINS 4 ALLOW FILTERING"));
		assertEquals(List.of(2),
			ints("SELECT id FROM query.writes WHERE m CONTAINS KEY 'b' ALLOW FILTERING"));
		assertEquals(List.of(),
			ints("SELECT id FROM query.writes WHERE l CONTAINS 99 ALLOW FILTERING"));

		assertInvalid("SELECT id FROM query.writes WHERE n CONTAINS 'text' ALLOW FILTERING", "n");
		assertInvalid("SELECT id FROM query.writes WHERE l CONTAINS KEY 1 ALLOW FILTERING", "l");
		// CONTAINS reads the whole collection, so it is a scan whatever else is restricted.
		assertInvalid("SELECT id FROM query.writes WHERE id = 1 AND l CONTAINS 2", "ALLOW FILTERING");
	}

	@Test
	@Order(126)
	@DisplayName("An operator no table can answer is rejected rather than ignored")
	void testUnanswerableOperators() {
		createCollectionTable();

		assertInvalid("SELECT id FROM query.writes WHERE n LIKE 'te%' ALLOW FILTERING", "n");
		assertInvalid("SELECT id FROM query.writes WHERE n IS NOT NULL ALLOW FILTERING", "n");
		assertInvalid("SELECT id FROM query.writes WHERE id != 1 ALLOW FILTERING", "id");
	}

	@Test
	@Order(127)
	@DisplayName("DELETE removes a whole partition, and a range of clustering rows within one")
	void testPartitionAndRangeDelete() {
		createEventTable();

		session.execute("DELETE FROM query.evt WHERE pk = 1 AND ck1 > 2");
		assertEquals(List.of("a", "b", "c"), texts("SELECT v FROM query.evt WHERE pk = 1"));

		session.execute("DELETE FROM query.evt WHERE pk = 1 AND ck1 = 1");
		assertEquals(List.of("c"), texts("SELECT v FROM query.evt WHERE pk = 1"));

		session.execute("DELETE FROM query.evt WHERE pk = 1");
		assertEquals(List.of(), texts("SELECT v FROM query.evt WHERE pk = 1"));
		assertEquals(List.of("e"), texts("SELECT v FROM query.evt WHERE pk = 2"));
	}

	@Test
	@Order(128)
	@DisplayName("DELETE needs the whole partition key, and the whole primary key to clear a column")
	void testDeleteRestrictionRules() {
		createEventTable();

		assertInvalid("DELETE FROM query.evt WHERE ck1 = 1", "pk");
		assertInvalid("DELETE FROM query.evt WHERE pk = 1 AND v = 'a'", "v");
		// Clearing a named column writes into a row, so it cannot address a range of them.
		assertInvalid("DELETE v FROM query.evt WHERE pk = 1", "columns");
		assertInvalid("DELETE v FROM query.evt WHERE pk = 1 AND ck1 > 1", "columns");

		session.execute("DELETE v FROM query.evt WHERE pk = 1 AND ck1 = 1 AND ck2 = 1");
		final var row = session.execute(
			"SELECT v FROM query.evt WHERE pk = 1 AND ck1 = 1 AND ck2 = 1").one();
		assertNotNull(row);
		assertNull(row.getString("v"));
	}

	@Test
	@Order(129)
	@DisplayName("UPDATE needs the whole primary key pinned by equality or IN")
	void testUpdateRestrictionRules() {
		createEventTable();

		assertInvalid("UPDATE query.evt SET v = 'z' WHERE pk = 1", "ck1");
		assertInvalid("UPDATE query.evt SET v = 'z' WHERE pk = 1 AND ck1 = 1", "ck2");
		assertInvalid("UPDATE query.evt SET v = 'z' WHERE pk = 1 AND ck1 = 1 AND ck2 > 1", "Slice");
		assertInvalid("UPDATE query.evt SET v = 'z' WHERE ck1 = 1 AND ck2 = 1", "pk");
		assertInvalid("UPDATE query.evt SET v = 'z' WHERE pk = 1 AND ck1 = 1 AND ck2 = 1 AND v = 'a'",
			"v");

		session.execute("UPDATE query.evt SET v = 'z' WHERE pk = 1 AND ck1 IN (1, 2) AND ck2 = 1");
		assertEquals(List.of("b", "d", "z", "z"), texts("SELECT v FROM query.evt WHERE pk = 1"));
	}

	@Test
	@Order(130)
	@DisplayName("A collection column is appended to, prepended to and discarded from")
	void testCollectionAssignments() {
		createCollectionTable();
		session.execute("INSERT INTO query.writes (id, l, s, m) VALUES (10, [2], {2}, {'b': 2})");

		session.execute("UPDATE query.writes SET l = l + [3] WHERE id = 10");
		session.execute("UPDATE query.writes SET l = [1] + l WHERE id = 10");
		session.execute("UPDATE query.writes SET s = s + {1, 3} WHERE id = 10");
		session.execute("UPDATE query.writes SET m = m + {'a': 1} WHERE id = 10");
		session.execute("UPDATE query.writes SET m['c'] = 3 WHERE id = 10");

		var row = session.execute("SELECT * FROM query.writes WHERE id = 10").one();
		assertNotNull(row);
		assertEquals(List.of(1, 2, 3), row.getList("l", Integer.class));
		assertEquals(Set.of(1, 2, 3), row.getSet("s", Integer.class));
		assertEquals(Map.of("a", 1, "b", 2, "c", 3), row.getMap("m", String.class, Integer.class));

		session.execute("UPDATE query.writes SET l = l - [2] WHERE id = 10");
		session.execute("UPDATE query.writes SET s = s - {2} WHERE id = 10");
		// A map is discarded from by key, so the term is a set of keys.
		session.execute("UPDATE query.writes SET m = m - {'b'} WHERE id = 10");
		session.execute("UPDATE query.writes SET l[0] = 9 WHERE id = 10");

		row = session.execute("SELECT * FROM query.writes WHERE id = 10").one();
		assertNotNull(row);
		assertEquals(List.of(9, 3), row.getList("l", Integer.class));
		assertEquals(Set.of(1, 3), row.getSet("s", Integer.class));
		assertEquals(Map.of("a", 1, "c", 3), row.getMap("m", String.class, Integer.class));
	}

	@Test
	@Order(131)
	@DisplayName("An assignment form the column's type cannot take is rejected")
	void testAssignmentTypeErrors() {
		createCollectionTable();
		session.execute("INSERT INTO query.writes (id, l, n) VALUES (11, [1], 'text')");

		assertInvalid("UPDATE query.writes SET n = n + 'more' WHERE id = 11", "n");
		assertInvalid("UPDATE query.writes SET n['a'] = 1 WHERE id = 11", "n");
		assertInvalid("UPDATE query.writes SET s['a'] = 1 WHERE id = 11", "s");
		assertInvalid("UPDATE query.writes SET l[9] = 1 WHERE id = 11", "l");
		assertInvalid("DELETE n['a'] FROM query.writes WHERE id = 11", "n");
	}

	@Test
	@Order(132)
	@DisplayName("DELETE of one collection element leaves the rest of the collection alone")
	void testElementDeletion() {
		createCollectionTable();
		session.execute("INSERT INTO query.writes (id, l, s, m) VALUES "
			+ "(12, [1, 2, 3], {1, 2}, {'a': 1, 'b': 2})");

		session.execute("DELETE m['a'] FROM query.writes WHERE id = 12");
		session.execute("DELETE s[1] FROM query.writes WHERE id = 12");
		session.execute("DELETE l[0] FROM query.writes WHERE id = 12");

		final var row = session.execute("SELECT * FROM query.writes WHERE id = 12").one();
		assertNotNull(row);
		assertEquals(List.of(2, 3), row.getList("l", Integer.class));
		assertEquals(Set.of(2), row.getSet("s", Integer.class));
		assertEquals(Map.of("b", 2), row.getMap("m", String.class, Integer.class));
	}

	@Test
	@Order(133)
	@DisplayName("A field of an unfrozen user defined type is written and cleared on its own")
	void testUserDefinedTypeFieldAssignment() {
		session.execute("CREATE TYPE IF NOT EXISTS query.addr (street text, city text)");
		session.execute("CREATE TABLE IF NOT EXISTS query.udt_writes "
			+ "(id int PRIMARY KEY, home addr, frozen_home frozen<addr>)");
		session.execute("INSERT INTO query.udt_writes (id, home, frozen_home) VALUES "
			+ "(1, {street: 'Main', city: 'Anytown'}, {street: 'Main', city: 'Anytown'})");

		session.execute("UPDATE query.udt_writes SET home.city = 'Elsewhere' WHERE id = 1");
		var home = session.execute("SELECT home FROM query.udt_writes WHERE id = 1").one()
			.getUdtValue("home");
		assertNotNull(home);
		assertEquals("Main", home.getString("street"));
		assertEquals("Elsewhere", home.getString("city"));

		session.execute("DELETE home.street FROM query.udt_writes WHERE id = 1");
		home = session.execute("SELECT home FROM query.udt_writes WHERE id = 1").one()
			.getUdtValue("home");
		assertNotNull(home);
		assertNull(home.getString("street"));
		assertEquals("Elsewhere", home.getString("city"));

		// A frozen value is one cell, so no field of it can be written on its own.
		assertInvalid("UPDATE query.udt_writes SET frozen_home.city = 'Nowhere' WHERE id = 1",
			"frozen_home");
		assertInvalid("DELETE frozen_home.city FROM query.udt_writes WHERE id = 1", "frozen_home");
		assertInvalid("UPDATE query.udt_writes SET home.nope = 'x' WHERE id = 1", "nope");
	}

	@Test
	@Order(134)
	@DisplayName("A counter column is incremented and decremented, and cannot be set or inserted")
	void testCounters() {
		session.execute("CREATE TABLE IF NOT EXISTS query.counters (pk int PRIMARY KEY, n counter)");

		// A counter that has never been written counts as zero, so the first increment creates it.
		session.execute("UPDATE query.counters SET n = n + 3 WHERE pk = 1");
		session.execute("UPDATE query.counters SET n = n + 4 WHERE pk = 1");
		session.execute("UPDATE query.counters SET n = n - 2 WHERE pk = 1");

		final var row = session.execute("SELECT n FROM query.counters WHERE pk = 1").one();
		assertNotNull(row);
		assertEquals(5L, row.getLong("n"));

		assertInvalid("UPDATE query.counters SET n = 5 WHERE pk = 1", "n");
		assertInvalid("INSERT INTO query.counters (pk, n) VALUES (2, 1)", "counter");
		assertInvalid("UPDATE query.counters SET n = n + 1 WHERE pk = 1 IF n = 5", "n");

		session.execute("DELETE FROM query.counters WHERE pk = 1");
		assertNull(session.execute("SELECT n FROM query.counters WHERE pk = 1").one());
	}

	@Test
	@Order(135)
	@DisplayName("A counter column cannot be part of a key or share a table with an ordinary column")
	void testCounterTableShape() {
		assertInvalid("CREATE TABLE query.counter_mixed (pk int PRIMARY KEY, n counter, t text)",
			"counter");
		assertInvalid("CREATE TABLE query.counter_key (pk counter PRIMARY KEY, n int)", "pk");
		assertInvalid(
			"CREATE TABLE query.counter_clustering (pk int, ck counter, n counter, PRIMARY KEY (pk, ck))",
			"ck");
		assertDoesNotThrow(() -> session.execute("CREATE TABLE IF NOT EXISTS query.counter_pair "
			+ "(pk int PRIMARY KEY, a counter, b counter)"));

		// A table the statement was refused for was never created.
		assertTrue(session.getMetadata().getKeyspace("query").orElseThrow().getTable("counter_mixed")
			.isEmpty());
	}

	@Test
	@Order(136)
	@DisplayName("A range compares by the column's type, and only types that have an order")
	void testRangeUsesColumnType() {
		session.execute("CREATE TABLE IF NOT EXISTS query.ordering "
			+ "(id int PRIMARY KEY, b blob, i inet, d duration, l list<int>)");
		session.execute("INSERT INTO query.ordering (id, b, i) VALUES (1, 0x01, '10.0.0.1')");
		session.execute("INSERT INTO query.ordering (id, b, i) VALUES (2, 0x80, '200.0.0.1')");

		// Bytes order unsigned, so 0x80 is above 0x7f rather than below it as a signed byte would be,
		// and an address orders by its bytes, which InetAddress itself does not answer for.
		assertEquals(List.of(2), ints("SELECT id FROM query.ordering WHERE b > 0x7f ALLOW FILTERING"));
		assertEquals(List.of(1, 2),
			ints("SELECT id FROM query.ordering WHERE i > '1.0.0.0' ALLOW FILTERING"));
		assertEquals(List.of(2),
			ints("SELECT id FROM query.ordering WHERE i > '100.0.0.0' ALLOW FILTERING"));

		assertInvalid("SELECT id FROM query.ordering WHERE d > 1m ALLOW FILTERING", "d");
		assertInvalid("SELECT id FROM query.ordering WHERE l > [1] ALLOW FILTERING", "l");
	}

	@Test
	@Order(244)
	@DisplayName("A row answers allIndicesOf and rejects an unknown column")
	void testRowAllIndicesOf() {
		session.execute("CREATE TABLE IF NOT EXISTS query.indices (id int PRIMARY KEY, name text)");
		session.execute("INSERT INTO query.indices (id, name) VALUES (1, 'a')");

		final var result = session.execute("SELECT id, name FROM query.indices WHERE id = 1");
		// The column definitions answer an unknown name with an empty list; the row throws.
		assertEquals(List.of(1), result.getColumnDefinitions().allIndicesOf("name"));
		assertEquals(List.of(), result.getColumnDefinitions().allIndicesOf("nope"));

		final var row = result.one();
		assertEquals(List.of(0), row.allIndicesOf("id"));
		assertThrows(IllegalArgumentException.class, () -> row.allIndicesOf("nope"));
	}

}
