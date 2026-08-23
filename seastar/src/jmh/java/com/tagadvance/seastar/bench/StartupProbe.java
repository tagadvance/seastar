package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.SeaStarDriverContext;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;

/**
 * One cold-JVM sample of SeaStar startup. Run by {@link ColdJvmBenchmark}, which forks a fresh JVM
 * per sample so the class loading that dominates the cold number is never warmed away.
 *
 * <p>Usage: {@code StartupProbe [plain|schema|clinitFirst|parseFirst|memory] [rows]}
 */
public final class StartupProbe {

	private static final String FIRST_QUERY =
		"CREATE KEYSPACE probe WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";

	private static final int WARM_SAMPLES = 10;

	private StartupProbe() {
	}

	public static void main(final String[] args) throws ClassNotFoundException, IOException {
		// First statement of main, before any reference to a com.tagadvance or cassandra-all class,
		// so that the harness's own clock excludes the JVM's own boot - every test JVM pays that
		// whether or not it uses SeaStar, so it must not count against any backend.
		final var mainStart = System.nanoTime();
		Metrics.millis("jvm.to.main", uptimeNanos());

		final var mode = args.length > 0 ? args[0] : "plain";
		switch (mode) {
			case "plain" -> plain(mainStart);
			case "schema" -> schema(mainStart);
			case "clinitFirst" -> parserSplit(true);
			case "parseFirst" -> parserSplit(false);
			case "memory" -> memory(args.length > 1 ? Integer.parseInt(args[1]) : 0);
			default -> throw new IllegalArgumentException(
				"mode must be plain, schema, clinitFirst, parseFirst or memory but was " + mode);
		}
	}

	/**
	 * Builds an empty session, then issues its first query. The first query is where the
	 * cassandra-all parser is loaded, so it is reported separately from the build.
	 */
	private static void plain(final long mainStart) {
		final var beforeBuild = System.nanoTime();
		final var session = SeaStarCqlSession.builder().build();
		final var afterBuild = System.nanoTime();
		Metrics.millis("build.cold", afterBuild - beforeBuild);
		Metrics.millis("jvm.to.build.cold", uptimeNanos());
		Metrics.millis("main.to.build.cold", afterBuild - mainStart);

		final var beforeQuery = System.nanoTime();
		session.execute(FIRST_QUERY);
		final var afterQuery = System.nanoTime();
		Metrics.millis("query.first", afterQuery - beforeQuery);
		Metrics.millis("jvm.to.first.query", uptimeNanos());
		Metrics.millis("main.to.first.query", afterQuery - mainStart);

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
	private static void schema(final long mainStart) {
		final var cql = BenchmarkSchema.cql();

		final var beforeBuild = System.nanoTime();
		final var session = SeaStarCqlSession.builder().withSchema(cql).build();
		final var afterBuild = System.nanoTime();
		Metrics.millis("build.schema.cold", afterBuild - beforeBuild);
		Metrics.millis("jvm.to.schema.ready", uptimeNanos());
		Metrics.millis("main.to.schema.ready", afterBuild - mainStart);
		Metrics.count("schema.statements", BenchmarkSchema.statementCount());

		final var beforeQuery = System.nanoTime();
		session.execute("SELECT pk FROM bench_ks_0.table_0");
		final var afterQuery = System.nanoTime();
		Metrics.millis("query.first", afterQuery - beforeQuery);
		Metrics.millis("main.to.first.query", afterQuery - mainStart);

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

	/**
	 * The same cold start as {@link #plain()}, with the first query broken into the three things it
	 * actually pays for: {@code QueryProcessor}'s static initializer, the ANTLR parser, and the
	 * handler dispatch. It measures in situ - rather than by extrapolation - what SeaStar would save
	 * by parsing through {@code CQLFragmentParser} instead of {@code QueryProcessor}.
	 *
	 * <p>The two initializers share several hundred classes, so whichever runs first is charged for
	 * them. Running the split both ways brackets the answer: {@code clinitFirst} is the most that
	 * could be attributed to {@code QueryProcessor}, and parse-first is the marginal cost of keeping
	 * it once the parser - which is not optional - has been loaded anyway.
	 */
	private static void parserSplit(final boolean clinitFirst) throws ClassNotFoundException {
		final var beforeBuild = System.nanoTime();
		final var session = SeaStarCqlSession.builder().build();
		Metrics.millis("build.cold", System.nanoTime() - beforeBuild);

		if (clinitFirst) {
			loadQueryProcessor();
			parseDirect();
		} else {
			parseDirect();
			loadQueryProcessor();
		}

		final var beforeQuery = System.nanoTime();
		session.execute(FIRST_QUERY);
		Metrics.millis("query.remainder", System.nanoTime() - beforeQuery);
		Metrics.millis("jvm.to.first.query", uptimeNanos());

		session.close();
	}

	private static void loadQueryProcessor() throws ClassNotFoundException {
		final var before = System.nanoTime();
		Class.forName("org.apache.cassandra.cql3.QueryProcessor", true,
			StartupProbe.class.getClassLoader());
		Metrics.millis("queryProcessor.clinit", System.nanoTime() - before);
	}

	private static void parseDirect() {
		final var before = System.nanoTime();
		final var raw = CQLFragmentParser.parseAny(CqlParser::query, FIRST_QUERY, "query");
		Metrics.millis("parse.direct", System.nanoTime() - before);
		Metrics.count("parse.direct.raw.hash", raw.hashCode() == 0 ? 0 : 1);
	}

	/**
	 * Builds a session seeded with the fixture schema, loads {@code rows} rows into one extra table,
	 * and reports the heap retained and the process's RSS. Both are read before and after so the
	 * reported figure is the delta this session is responsible for, not whatever the JVM itself
	 * already holds.
	 */
	private static void memory(final int rows) throws IOException {
		final var rssBefore = readRssKb();
		gcSettle();
		final var heapBefore = heapUsedBytes();

		final var session = SeaStarCqlSession.builder().withSchema(BenchmarkSchema.cql()).build();
		session.execute(
			"CREATE KEYSPACE bench_mem WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		session.execute("CREATE TABLE bench_mem.rows (id int PRIMARY KEY, name text, age int)");

		if (rows > 0) {
			final SeaStarDriverContext context = session.getContext();
			final var table = context.getSeaStarKeyspace(CqlIdentifier.fromInternal("bench_mem"))
				.flatMap(keyspace -> keyspace.getSeaStarTable(CqlIdentifier.fromInternal("rows")))
				.orElseThrow(() -> new IllegalStateException("bench_mem.rows must exist after CREATE TABLE"));
			final var idIndex = table.firstIndexOf(CqlIdentifier.fromInternal("id"));
			final var nameIndex = table.firstIndexOf(CqlIdentifier.fromInternal("name"));
			final var ageIndex = table.firstIndexOf(CqlIdentifier.fromInternal("age"));
			for (int id = 0; id < rows; id++) {
				final var values = new ArrayList<Object>(Collections.nCopies(table.size(), null));
				values.set(idIndex, id);
				values.set(nameIndex, "name-" + id);
				values.set(ageIndex, id % 100);
				table.addRow(values);
			}
		}

		gcSettle();
		final var heapAfter = heapUsedBytes();
		final var rssAfter = readRssKb();

		Metrics.count("memory.rows", rows);
		Metrics.value("memory.heap.used.mb", (heapAfter - heapBefore) / (1024.0d * 1024.0d));
		Metrics.value("memory.rss.kb", rssAfter);
		Metrics.value("memory.rss.delta.kb", rssAfter - rssBefore);

		session.close();
	}

	private static void gcSettle() {
		for (int i = 0; i < 3; i++) {
			System.gc();
			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	private static long heapUsedBytes() {
		return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
	}

	/**
	 * Linux only - reads VmRSS out of {@code /proc/self/status}. Returns -1 if it cannot be read,
	 * rather than failing the probe over a metric that is inherently platform specific.
	 */
	private static long readRssKb() throws IOException {
		return Files.readAllLines(Path.of("/proc/self/status")).stream()
			.filter(line -> line.startsWith("VmRSS:"))
			.findFirst()
			.map(line -> Long.parseLong(line.replaceAll("[^0-9]", "")))
			.orElse(-1L);
	}

	private static long uptimeNanos() {
		return ManagementFactory.getRuntimeMXBean().getUptime() * 1_000_000L;
	}

	private static long median(final List<Long> values) {
		final var sorted = values.stream().sorted().toList();

		return sorted.get(sorted.size() / 2);
	}

}
