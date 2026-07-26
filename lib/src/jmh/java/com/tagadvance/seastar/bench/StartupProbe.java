package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * One cold-JVM sample of SeaStar startup. Run by {@link ColdJvmBenchmark}, which forks a fresh JVM
 * per sample so the class loading that dominates the cold number is never warmed away.
 *
 * <p>Usage: {@code StartupProbe [plain|schema]}
 */
public final class StartupProbe {

	private static final String FIRST_QUERY =
		"CREATE KEYSPACE probe WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";

	private static final int WARM_SAMPLES = 10;

	private StartupProbe() {
	}

	public static void main(final String[] args) {
		final var mode = args.length > 0 ? args[0] : "plain";
		switch (mode) {
			case "plain" -> plain();
			case "schema" -> schema();
			default -> throw new IllegalArgumentException(
				"mode must be plain or schema but was " + mode);
		}
	}

	/**
	 * Builds an empty session, then issues its first query. The first query is where the
	 * cassandra-all parser is loaded, so it is reported separately from the build.
	 */
	private static void plain() {
		final var beforeBuild = System.nanoTime();
		final var session = SeaStarCqlSession.builder().build();
		final var afterBuild = System.nanoTime();
		Metrics.millis("build.cold", afterBuild - beforeBuild);
		Metrics.millis("jvm.to.build.cold", uptimeNanos());

		final var beforeQuery = System.nanoTime();
		session.execute(FIRST_QUERY);
		final var afterQuery = System.nanoTime();
		Metrics.millis("query.first", afterQuery - beforeQuery);
		Metrics.millis("jvm.to.first.query", uptimeNanos());

		final List<Long> builds = new ArrayList<>();
		final List<Long> queries = new ArrayList<>();
		for (int i = 0; i < WARM_SAMPLES; i++) {
			final var beforeWarmBuild = System.nanoTime();
			final var warm = SeaStarCqlSession.builder().build();
			builds.add(System.nanoTime() - beforeWarmBuild);

			final var beforeWarmQuery = System.nanoTime();
			warm.execute(FIRST_QUERY);
			queries.add(System.nanoTime() - beforeWarmQuery);
			warm.close();
		}
		Metrics.millis("build.warm", median(builds));
		Metrics.millis("query.warm", median(queries));

		session.close();
	}

	/**
	 * Builds a session seeded with a realistic fixture schema, which is what a test suite actually
	 * pays at startup.
	 */
	private static void schema() {
		final var cql = BenchmarkSchema.cql();

		final var beforeBuild = System.nanoTime();
		final var session = SeaStarCqlSession.builder().withSchema(cql).build();
		final var afterBuild = System.nanoTime();
		Metrics.millis("build.schema.cold", afterBuild - beforeBuild);
		Metrics.millis("jvm.to.schema.ready", uptimeNanos());
		Metrics.count("schema.statements", BenchmarkSchema.statementCount());

		final var beforeQuery = System.nanoTime();
		session.execute("SELECT pk FROM bench_ks_0.table_0");
		Metrics.millis("query.first", System.nanoTime() - beforeQuery);

		final List<Long> builds = new ArrayList<>();
		for (int i = 0; i < WARM_SAMPLES; i++) {
			final var before = System.nanoTime();
			final CqlSession warm = SeaStarCqlSession.builder().withSchema(cql).build();
			builds.add(System.nanoTime() - before);
			warm.close();
		}
		Metrics.millis("build.schema.warm", median(builds));

		session.close();
	}

	private static long uptimeNanos() {
		return ManagementFactory.getRuntimeMXBean().getUptime() * 1_000_000L;
	}

	private static long median(final List<Long> values) {
		final var sorted = values.stream().sorted().toList();

		return sorted.get(sorted.size() / 2);
	}

}
