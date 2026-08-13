package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * The cells a row is made of: write timestamps, TTLs and how a write is resolved against what is
 * already stored; static columns; and the paging idioms SeaStar answers on one page. This group
 * owns the {@code cells} keyspace.
 */
public abstract class AbstractCellFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("cells");
	}

	@Test
	@Order(210)
	@DisplayName("writetime and ttl report a cell's metadata, and null for a cell holding nothing")
	void testWritetimeAndTtl() {
		session.execute("CREATE TABLE IF NOT EXISTS cells.wt (pk int PRIMARY KEY, v text, w text)");
		final var before = Instant.now().toEpochMilli() * 1000;
		session.execute("INSERT INTO cells.wt (pk, v) VALUES (1, 'a') USING TTL 3600");

		final var row = only("SELECT writetime(v), ttl(v), writetime(w), ttl(w) FROM cells.wt");
		final var writetime = row.getLong(0);
		assertTrue(writetime >= before, "writetime %d should be at or after %d".formatted(writetime,
			before));
		final var ttl = row.getInt(1);
		assertTrue(ttl > 3500 && ttl <= 3600, "ttl should be counting down from 3600 but was " + ttl);
		assertTrue(row.isNull(2), "writetime of a column holding nothing should be null");
		assertTrue(row.isNull(3), "ttl of a column holding nothing should be null");

		assertEquals(List.of("writetime(v)", "ttl(v)"),
			columnNames("SELECT writetime(v), ttl(v) FROM cells.wt"));
	}

	@Test
	@Order(211)
	@DisplayName("writetime and ttl are refused on a primary key part, which is not a cell")
	void testWritetimeOnPrimaryKey() {
		session.execute("CREATE TABLE IF NOT EXISTS cells.wtpk (pk int PRIMARY KEY, v text)");

		assertInvalid("SELECT writetime(pk) FROM cells.wtpk", "pk");
		assertInvalid("SELECT ttl(pk) FROM cells.wtpk", "pk");
	}

	@Test
	@Order(212)
	@DisplayName("A write stamped older than the value already stored is discarded")
	void testUsingTimestampResolvesConflicts() {
		session.execute("CREATE TABLE IF NOT EXISTS cells.ts (pk int PRIMARY KEY, v text)");
		session.execute("INSERT INTO cells.ts (pk, v) VALUES (1, 'new') USING TIMESTAMP 2000");
		assertEquals(2000L, only("SELECT writetime(v) FROM cells.ts WHERE pk = 1").getLong(0));

		session.execute("INSERT INTO cells.ts (pk, v) VALUES (1, 'old') USING TIMESTAMP 1000");
		assertEquals("new", only("SELECT v FROM cells.ts WHERE pk = 1").getString(0));

		session.execute("UPDATE cells.ts USING TIMESTAMP 3000 SET v = 'newer' WHERE pk = 1");
		assertEquals("newer", only("SELECT v FROM cells.ts WHERE pk = 1").getString(0));
		assertEquals(3000L, only("SELECT writetime(v) FROM cells.ts WHERE pk = 1").getLong(0));

		// A DELETE stamped older than the value it would remove leaves it alone.
		session.execute("DELETE FROM cells.ts USING TIMESTAMP 2500 WHERE pk = 1");
		assertEquals(1, session.execute("SELECT v FROM cells.ts WHERE pk = 1").all().size());
		session.execute("DELETE FROM cells.ts USING TIMESTAMP 4000 WHERE pk = 1");
		assertEquals(0, session.execute("SELECT v FROM cells.ts WHERE pk = 1").all().size());
	}

	@Test
	@Order(213)
	@DisplayName("A TTL of zero means no TTL, and a negative one is refused")
	void testTtlBounds() {
		session.execute("CREATE TABLE IF NOT EXISTS cells.ttlb (pk int PRIMARY KEY, v text)");
		session.execute("INSERT INTO cells.ttlb (pk, v) VALUES (1, 'a') USING TTL 0");

		assertTrue(only("SELECT ttl(v) FROM cells.ttlb WHERE pk = 1").isNull(0));
		assertInvalid("INSERT INTO cells.ttlb (pk, v) VALUES (2, 'b') USING TTL -1", "TTL");
	}

	@Test
	@Order(214)
	@DisplayName("A counter takes no custom TTL or timestamp")
	void testCounterRejectsUsing() {
		session.execute("CREATE TABLE IF NOT EXISTS cells.cnt (pk int PRIMARY KEY, c counter)");

		assertInvalid("UPDATE cells.cnt USING TTL 60 SET c = c + 1 WHERE pk = 1", "TTL");
		assertInvalid("UPDATE cells.cnt USING TIMESTAMP 1000 SET c = c + 1 WHERE pk = 1", "timestamp");
	}

	/**
	 * A partition with two clustered rows, which is the shape a static column is about: one value
	 * shared by the partition beside values that belong to each row.
	 */
	private void createStaticTable() {
		session.execute("CREATE TABLE IF NOT EXISTS cells.st "
			+ "(pk int, ck int, st text static, v text, PRIMARY KEY (pk, ck))");
	}

	@Test
	@Order(215)
	@DisplayName("A static column is shared by every row of its partition")
	void testStaticColumnsAreShared() {
		createStaticTable();
		session.execute("INSERT INTO cells.st (pk, ck, v) VALUES (1, 1, 'a')");
		session.execute("INSERT INTO cells.st (pk, ck, v) VALUES (1, 2, 'b')");
		session.execute("INSERT INTO cells.st (pk, ck, v, st) VALUES (1, 3, 'c', 'shared')");

		assertEquals(List.of("shared", "shared", "shared"),
			texts("SELECT st FROM cells.st WHERE pk = 1"));

		// Another partition keeps its own value, which is what makes it a partition-level cell.
		session.execute("INSERT INTO cells.st (pk, ck, v, st) VALUES (2, 1, 'd', 'other')");
		assertEquals(List.of("other"), texts("SELECT st FROM cells.st WHERE pk = 2"));
		assertEquals(List.of("shared", "shared", "shared"),
			texts("SELECT st FROM cells.st WHERE pk = 1"));

		session.execute("UPDATE cells.st SET st = 'changed' WHERE pk = 1");
		assertEquals(List.of("changed", "changed", "changed"),
			texts("SELECT st FROM cells.st WHERE pk = 1"));

		session.execute("DELETE st FROM cells.st WHERE pk = 1");
		assertEquals(3, session.execute("SELECT st FROM cells.st WHERE pk = 1")
			.all()
			.stream()
			.filter(row -> row.isNull(0))
			.count());
	}

	@Test
	@Order(216)
	@DisplayName("A static-only INSERT needs no clustering key and reads back with a null one")
	void testStaticOnlyInsert() {
		createStaticTable();
		session.execute("INSERT INTO cells.st (pk, st) VALUES (7, 'only-static')");

		final var row = only("SELECT pk, ck, st, v FROM cells.st WHERE pk = 7");
		assertEquals(7, row.getInt("pk"));
		assertTrue(row.isNull("ck"), "a static-only row has no clustering key");
		assertEquals("only-static", row.getString("st"));
		assertTrue(row.isNull("v"));

		// Once the partition has a clustered row, that row is what it answers with.
		session.execute("INSERT INTO cells.st (pk, ck, v) VALUES (7, 1, 'x')");
		final var clustered = session.execute("SELECT ck, st, v FROM cells.st WHERE pk = 7").all();
		assertEquals(1, clustered.size());
		assertEquals(1, clustered.get(0).getInt("ck"));
		assertEquals("only-static", clustered.get(0).getString("st"));
	}

	@Test
	@Order(217)
	@DisplayName("An UPDATE that writes only static columns cannot restrict a clustering column")
	void testStaticUpdateRestrictions() {
		createStaticTable();
		session.execute("INSERT INTO cells.st (pk, ck, v) VALUES (5, 1, 'a')");

		assertInvalid("UPDATE cells.st SET st = 'x' WHERE pk = 5 AND ck = 1", "static");
		assertDoesNotThrow(() -> session.execute("UPDATE cells.st SET st = 'x' WHERE pk = 5"));
		assertDoesNotThrow(
			() -> session.execute("UPDATE cells.st SET st = 'y', v = 'b' WHERE pk = 5 AND ck = 1"));
		assertEquals(List.of("y"), texts("SELECT st FROM cells.st WHERE pk = 5"));
	}

	@Test
	@Order(218)
	@DisplayName("Deleting a partition takes its static columns with it")
	void testStaticColumnsGoWithThePartition() {
		createStaticTable();
		session.execute("INSERT INTO cells.st (pk, ck, v, st) VALUES (8, 1, 'a', 'gone')");

		session.execute("DELETE FROM cells.st WHERE pk = 8");
		assertEquals(0, session.execute("SELECT st FROM cells.st WHERE pk = 8").all().size());

		session.execute("INSERT INTO cells.st (pk, ck, v) VALUES (8, 1, 'a')");
		assertTrue(only("SELECT st FROM cells.st WHERE pk = 8").isNull(0),
			"a re-created partition should not read the static value the deleted one held");
	}

	@Test
	@Order(219)
	@DisplayName("SELECT DISTINCT reads one row per partition, keys and static columns only")
	void testDistinctWithStatics() {
		createStaticTable();
		session.execute("INSERT INTO cells.st (pk, ck, v, st) VALUES (11, 1, 'a', 's11')");
		session.execute("INSERT INTO cells.st (pk, ck, v, st) VALUES (11, 2, 'b', 's11')");

		assertEquals(List.of("s11"), texts("SELECT DISTINCT st FROM cells.st WHERE pk = 11"));
		assertInvalid("SELECT DISTINCT v FROM cells.st", "v");
	}

	@Test
	@Order(220)
	@DisplayName("The paging idioms terminate and return every row")
	void testPagingIdiomsTerminate() throws Exception {
		session.execute("CREATE TABLE IF NOT EXISTS cells.pages (pk int, ck int, PRIMARY KEY (pk, ck))");
		for (int i = 1; i <= 5; i++) {
			session.execute("INSERT INTO cells.pages (pk, ck) VALUES (1, %d)".formatted(i));
		}

		final var statement = SimpleStatement.newInstance("SELECT ck FROM cells.pages WHERE pk = 1")
			.setPageSize(2);

		assertEquals(5, session.execute(statement).all().size());

		var counted = 0;
		for (final var ignored : session.execute(statement)) {
			counted++;
		}
		assertEquals(5, counted);

		final List<Integer> fetched = new ArrayList<>();
		var page = session.executeAsync(statement).toCompletableFuture().get();
		while (true) {
			page.currentPage().forEach(row -> fetched.add(row.getInt(0)));
			if (!page.hasMorePages()) {
				break;
			}
			page = page.fetchNextPage().toCompletableFuture().get();
		}
		assertEquals(List.of(1, 2, 3, 4, 5), fetched);
	}

}
