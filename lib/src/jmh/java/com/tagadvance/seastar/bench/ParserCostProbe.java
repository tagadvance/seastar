package com.tagadvance.seastar.bench;

import java.lang.management.ManagementFactory;
import java.util.List;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QueryProcessor;

/**
 * Breaks the one-time cost of the first CQL parse in a JVM into its parts, so it can be attributed
 * rather than guessed at.
 *
 * <p>{@code QueryProcessor.parseStatement} is a one-line delegate to
 * {@link CQLFragmentParser#parseAnyUnhandled}, but calling it triggers {@code QueryProcessor}'s
 * static initializer, which reads {@code DatabaseDescriptor}, builds a Caffeine prepared-statement
 * cache and schedules a task on Cassandra's shared scheduler. The {@code direct} mode measures the
 * same parse without ever loading {@code QueryProcessor}; the difference between the two modes is
 * what SeaStar pays for using the convenience entry point.
 *
 * <p>Usage: {@code ParserCostProbe [queryProcessor|direct|clinitOnly|equivalence]}
 */
public final class ParserCostProbe {

	private static final String QUERY = "SELECT pk, label FROM bench_ks_0.table_0 WHERE pk = ?";

	private static final String SECOND_QUERY =
		"INSERT INTO bench_ks_0.table_0 (pk, ck, label) VALUES (?, ?, ?)";

	private ParserCostProbe() {
	}

	public static void main(final String[] args) throws ClassNotFoundException {
		final var mode = args.length > 0 ? args[0] : "queryProcessor";
		switch (mode) {
			case "queryProcessor" -> viaQueryProcessor();
			case "direct" -> direct();
			case "clinitOnly" -> clinitOnly();
			case "equivalence" -> equivalence();
			default -> throw new IllegalArgumentException(
				"mode must be queryProcessor, direct, clinitOnly or equivalence but was " + mode);
		}
	}

	private static void viaQueryProcessor() {
		final var before = classes();
		final var beforeNanos = System.nanoTime();
		final CQLStatement.Raw first = QueryProcessor.parseStatement(QUERY);
		final var afterNanos = System.nanoTime();
		report("parse.first", beforeNanos, afterNanos, before, first);

		final var beforeSecond = System.nanoTime();
		final CQLStatement.Raw second = QueryProcessor.parseStatement(SECOND_QUERY);
		Metrics.millis("parse.second", System.nanoTime() - beforeSecond);
		Metrics.count("parse.second.raw.hash", second.hashCode() == 0 ? 0 : 1);

		Metrics.count("threads", ManagementFactory.getThreadMXBean().getThreadCount());
		Metrics.millis("jvm.total", uptimeNanos());
	}

	private static void direct() {
		final var before = classes();
		final var beforeNanos = System.nanoTime();
		final CQLStatement.Raw first = CQLFragmentParser.parseAny(CqlParser::query, QUERY, "query");
		final var afterNanos = System.nanoTime();
		report("parse.first", beforeNanos, afterNanos, before, first);

		final var beforeSecond = System.nanoTime();
		final CQLStatement.Raw second = CQLFragmentParser.parseAny(CqlParser::query, SECOND_QUERY,
			"query");
		Metrics.millis("parse.second", System.nanoTime() - beforeSecond);
		Metrics.count("parse.second.raw.hash", second.hashCode() == 0 ? 0 : 1);

		Metrics.count("threads", ManagementFactory.getThreadMXBean().getThreadCount());
		Metrics.millis("jvm.total", uptimeNanos());
	}

	/**
	 * Loads {@code QueryProcessor} and runs its static initializer without parsing anything, which
	 * isolates the initializer from the ANTLR class loading that follows it.
	 */
	private static void clinitOnly() throws ClassNotFoundException {
		final var before = classes();
		final var beforeNanos = System.nanoTime();
		Class.forName("org.apache.cassandra.cql3.QueryProcessor", true,
			ParserCostProbe.class.getClassLoader());
		final var afterNanos = System.nanoTime();
		Metrics.millis("queryProcessor.clinit", afterNanos - beforeNanos);
		Metrics.count("queryProcessor.clinit.classes", classes() - before);
		Metrics.count("threads", ManagementFactory.getThreadMXBean().getThreadCount());
		Metrics.millis("jvm.total", uptimeNanos());
	}

	/**
	 * Evidence that the two entry points are interchangeable: {@code QueryProcessor.parseStatement}
	 * is a delegate to {@link CQLFragmentParser}, so both must yield the same parse tree type for
	 * every statement SeaStar handles.
	 */
	private static void equivalence() {
		final var queries = List.of(
			"CREATE KEYSPACE ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
			"CREATE TYPE ks.address (street text, zip int)",
			"ALTER TYPE ks.address ADD city text",
			"CREATE TABLE ks.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))",
			"CREATE INDEX ON ks.t (v)",
			"USE ks",
			"INSERT INTO ks.t (pk, ck, v) VALUES (?, ?, ?)",
			"UPDATE ks.t SET v = ? WHERE pk = ? AND ck = ? IF EXISTS",
			"DELETE v FROM ks.t WHERE pk = ? AND ck = ?",
			"SELECT DISTINCT pk FROM ks.t WHERE pk = ? LIMIT 10 ALLOW FILTERING",
			"TRUNCATE ks.t",
			"DROP TABLE IF EXISTS ks.t",
			"DROP KEYSPACE IF EXISTS ks",
			"BEGIN BATCH INSERT INTO ks.t (pk, ck) VALUES (1, 2); APPLY BATCH");

		final var mismatches = queries.stream()
			.filter(query -> !QueryProcessor.parseStatement(query).getClass()
				.equals(CQLFragmentParser.parseAny(CqlParser::query, query, "query").getClass()))
			.count();
		Metrics.count("equivalence.queries", queries.size());
		Metrics.count("equivalence.mismatches", mismatches);
	}

	private static void report(final String name, final long beforeNanos, final long afterNanos,
		final long beforeClasses, final CQLStatement.Raw raw) {
		Metrics.millis(name, afterNanos - beforeNanos);
		Metrics.count(name + ".classes", classes() - beforeClasses);
		// Keeps the parse result reachable so nothing about it can be optimized away.
		Metrics.count(name + ".raw.hash", raw.hashCode() == 0 ? 0 : 1);
	}

	private static long classes() {
		return ManagementFactory.getClassLoadingMXBean().getTotalLoadedClassCount();
	}

	private static long uptimeNanos() {
		return ManagementFactory.getRuntimeMXBean().getUptime() * 1_000_000L;
	}

}
