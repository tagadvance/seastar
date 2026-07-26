package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.IntStream;

/**
 * Shared fixture construction for the JMH benchmarks: one keyspace and a table seeded with a known
 * number of rows.
 */
final class BenchmarkFixture {

	static final String KEYSPACE = "bench";

	private BenchmarkFixture() {
	}

	static CqlSession newSession() {
		final var session = SeaStarCqlSession.builder().build();
		session.execute("CREATE KEYSPACE " + KEYSPACE
			+ " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

		return session;
	}

	/**
	 * Creates {@code keyspace.table} as {@code (id int PRIMARY KEY, name text, age int)} and fills it
	 * with {@code rows} rows keyed {@code 0..rows-1}.
	 *
	 * <p>Rows go in through the storage model rather than through INSERT: INSERT itself scans the
	 * table for an existing primary key, so seeding 100k rows by CQL is quadratic and would take
	 * longer than the benchmark it sets up.
	 */
	static void seed(final CqlSession session, final String table, final int rows) {
		session.execute("CREATE TABLE %s.%s (id int PRIMARY KEY, name text, age int)"
			.formatted(KEYSPACE, table));

		final var context = (SeaStarDriverContext) session.getContext();
		final var target = context.getSeaStarKeyspace(CqlIdentifier.fromInternal(KEYSPACE))
			.flatMap(keyspace -> keyspace.getSeaStarTable(CqlIdentifier.fromInternal(table)))
			.orElseThrow(() -> new IllegalStateException(
				"the seeded table %s must exist immediately after CREATE TABLE".formatted(table)));
		// Column order is whatever the model chose, so place each value by its own index.
		final var idIndex = target.firstIndexOf(CqlIdentifier.fromInternal("id"));
		final var nameIndex = target.firstIndexOf(CqlIdentifier.fromInternal("name"));
		final var ageIndex = target.firstIndexOf(CqlIdentifier.fromInternal("age"));
		IntStream.range(0, rows).forEach(id -> {
			final var values = new ArrayList<Object>(Collections.nCopies(target.size(), null));
			values.set(idIndex, id);
			values.set(nameIndex, "name-" + id);
			values.set(ageIndex, id % 100);
			target.addRow(values);
		});
	}

	static String qualify(final String table) {
		return KEYSPACE + '.' + table;
	}

}
