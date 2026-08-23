package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

/**
 * One cold-JVM sample of cassandra-unit: a real Cassandra 3.11.5 node embedded in the test JVM,
 * on driver 4.3.1. Run standalone - this module deliberately has no dependency on anything else in
 * this build (see {@code seastar/build.gradle.kts}, the {@code cassandraUnitBench} source set),
 * because cassandra-all 3.11.5 and driver 4.3.1 would shadow the pinned 5.0.8/4.19.3 versions the
 * same way TestContainers' 3.x driver does. That is also why this duplicates rather than reuses
 * {@code BenchmarkSchema} and {@code Metrics}: reaching into the {@code jmh} source set would pull
 * bytecode compiled for a newer release than this class targets, and would put a modern-classpath
 * class on a JDK 8 JVM's boot path.
 *
 * <p>{@link ColdJvmBenchmark} runs this from the outside, forking a JDK 8 {@code java} - the only
 * JDK Cassandra 3.11 supports - via {@code probe.javaLauncher}, with this source set's own
 * classpath via {@code probe.classpath}. Both are set by the {@code cassandraUnitBenchmark} Gradle
 * task; see there for why a JDK 8 <em>compiler</em> could not be used the same way (no
 * {@code --release} flag).
 *
 * <p>Every clock follows the M1 convention: {@code mainStart} is the first statement of
 * {@code main}. cassandra-unit leaves non-daemon threads running after
 * {@link EmbeddedCassandraServerHelper#stopEmbeddedCassandra()}, so this calls
 * {@link System#exit} at the end rather than falling off the end of {@code main} - otherwise
 * {@link ColdJvmBenchmark} would wait on a JVM that never terminates.
 */
public final class CassandraUnitProbe {

	private static final String METRIC_PREFIX = "METRIC";

	private static final long STARTUP_TIMEOUT_MS = 120_000L;

	private static final int WARM_SAMPLES = 10;

	private static final int KEYSPACES = 5;

	private static final int TABLES_PER_KEYSPACE = 10;

	private CassandraUnitProbe() {
	}

	public static void main(final String[] args) throws Exception {
		final long mainStart = System.nanoTime();

		final long beforeBoot = System.nanoTime();
		EmbeddedCassandraServerHelper.startEmbeddedCassandra(STARTUP_TIMEOUT_MS);
		final long afterBoot = System.nanoTime();
		millis("boot", afterBoot - beforeBoot);
		millis("main.to.boot", afterBoot - mainStart);

		// Shortened the same way the wire and container backends are: left at its one second
		// default, replaying 75 DDL statements below would spend ~75 s in the debounce window
		// rather than measuring cassandra-unit.
		final DriverConfigLoader config = DriverConfigLoader.programmaticBuilder()
			.withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofMillis(1))
			.build();

		final long beforeConnect = System.nanoTime();
		final CqlSession session = CqlSession.builder()
			.addContactPoint(new InetSocketAddress(EmbeddedCassandraServerHelper.getHost(),
				EmbeddedCassandraServerHelper.getNativeTransportPort()))
			.withLocalDatacenter("datacenter1")
			.withConfigLoader(config)
			.build();
		final long afterConnect = System.nanoTime();
		millis("connect", afterConnect - beforeConnect);
		millis("main.to.connect", afterConnect - mainStart);

		try {
			runSchemaAndQuery(session, mainStart);
			memory();
		} finally {
			session.close();
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		}

		System.exit(0);
	}

	/**
	 * Replays the fixture schema one statement at a time, recording which - if any - 3.11.5
	 * rejects, then runs the point query the other backends use, warm and cold.
	 */
	private static void runSchemaAndQuery(final CqlSession session, final long mainStart) {
		final List<String> statements = schema();
		int rejected = 0;
		final StringBuilder rejections = new StringBuilder();

		final long beforeSchema = System.nanoTime();
		for (int i = 0; i < statements.size(); i++) {
			try {
				session.execute(statements.get(i));
			} catch (final RuntimeException e) {
				rejected++;
				rejections.append(i).append(':').append(e.getClass().getSimpleName()).append(':')
					.append(String.valueOf(e.getMessage())).append(" | ");
			}
		}
		final long afterSchema = System.nanoTime();
		millis("schema", afterSchema - beforeSchema);
		millis("main.to.schema.ready", afterSchema - mainStart);
		count("schema.statements", statements.size());
		count("schema.rejected", rejected);
		if (rejected > 0) {
			// Not a METRIC line on purpose: ColdJvmBenchmark parses every METRIC-prefixed line's
			// third field as a Double, and this is prose. Run the probe directly (not through the
			// cold-JVM harness) to see it - ColdJvmBenchmark only aggregates numeric metrics.
			System.out.println("REJECTIONS\t" + rejections);
		}

		try {
			final long beforeQuery = System.nanoTime();
			session.execute("SELECT pk FROM bench_ks_0.table_0");
			final long afterQuery = System.nanoTime();
			millis("query.first", afterQuery - beforeQuery);
			millis("main.to.first.query", afterQuery - mainStart);

			final List<Long> warm = new ArrayList<Long>();
			for (int i = 0; i < WARM_SAMPLES; i++) {
				final long before = System.nanoTime();
				session.execute("SELECT pk FROM bench_ks_0.table_0");
				warm.add(System.nanoTime() - before);
			}
			millis("query.warm", median(warm));
		} catch (final RuntimeException e) {
			System.out.println("QUERY_ERROR\t" + e.getClass().getSimpleName() + ": " + e.getMessage());
		}
	}

	private static void memory() throws IOException, InterruptedException {
		for (int i = 0; i < 3; i++) {
			System.gc();
			Thread.sleep(100);
		}
		final long heapUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
		value("memory.heap.used.mb", heapUsed / (1024.0d * 1024.0d));
		value("memory.rss.kb", (double) readRssKb());
	}

	/**
	 * Linux only - reads VmRSS out of {@code /proc/self/status}.
	 */
	private static long readRssKb() throws IOException {
		final List<String> lines = Files.readAllLines(Paths.get("/proc/self/status"));
		for (final String line : lines) {
			if (line.startsWith("VmRSS:")) {
				return Long.parseLong(line.replaceAll("[^0-9]", ""));
			}
		}

		return -1L;
	}

	/**
	 * The same shape {@code BenchmarkSchema} generates - {@value #KEYSPACES} keyspaces,
	 * {@value #TABLES_PER_KEYSPACE} tables each, two UDTs and two secondary indexes per keyspace,
	 * 75 statements - reproduced here rather than shared, per the class javadoc.
	 */
	private static List<String> schema() {
		final List<String> statements = new ArrayList<String>();
		for (int k = 0; k < KEYSPACES; k++) {
			final String name = "bench_ks_" + k;
			statements.add("CREATE KEYSPACE " + name
				+ " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			statements.add("CREATE TYPE " + name + ".address (street text, city text, zip int)");
			statements.add("CREATE TYPE " + name + ".contact (email text, phone text)");
			for (int t = 0; t < TABLES_PER_KEYSPACE; t++) {
				statements.add(table(name, t));
			}
			statements.add("CREATE INDEX ON " + name + ".table_0 (label)");
			statements.add("CREATE INDEX ON " + name + ".table_1 (label)");
		}

		return statements;
	}

	private static String table(final String keyspace, final int index) {
		final String udtColumn = index % 3 == 0 ? "home frozen<address>,\n\t" : "";

		return "CREATE TABLE " + keyspace + ".table_" + index + " (\n"
			+ "\tpk uuid,\n"
			+ "\tck timestamp,\n"
			+ "\tlabel text,\n"
			+ "\tamount double,\n"
			+ "\tquantity int,\n"
			+ "\tactive boolean,\n"
			+ '\t' + udtColumn + "PRIMARY KEY (pk, ck)\n"
			+ ")";
	}

	private static long median(final List<Long> values) {
		final List<Long> sorted = new ArrayList<Long>(values);
		Collections.sort(sorted);

		return sorted.get(sorted.size() / 2);
	}

	private static void millis(final String name, final long nanos) {
		value(name, nanos / 1_000_000.0d);
	}

	private static void count(final String name, final long value) {
		value(name, (double) value);
	}

	private static void value(final String name, final double v) {
		System.out.println(METRIC_PREFIX + "\t" + name + "\t" + v);
	}

}
