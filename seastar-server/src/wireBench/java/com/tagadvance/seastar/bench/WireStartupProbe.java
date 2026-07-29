package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.server.SeaStarProtocolServer;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * One cold-JVM sample of what the wire costs. Run by {@link ColdJvmBenchmark}, which forks a fresh
 * JVM per sample so that the class loading dominating a cold number is never warmed away - and here
 * that is Netty's and the driver's request pipeline as well as cassandra-all's parser.
 *
 * <p>This is deliberately the same shape as {@code StartupProbe}, and reports
 * {@code jvm.to.first.query} the same way, so that the two are read against each other: the
 * difference between them is the whole cost of putting a socket in the middle.
 *
 * <p>Every warm figure is measured twice in the same JVM, once over the socket and once against the
 * in-process session the listener is serving. The pair is the point. A statement costs SeaStar the
 * same either way, so what the wire column measures is the loopback round trip and the driver's
 * request pipeline, and publishing both is what stops the wire number reading as SeaStar being slow.
 */
public final class WireStartupProbe {

	private static final String CREATE_KEYSPACE =
		"CREATE KEYSPACE probe WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";

	private static final String CREATE_TABLE =
		"CREATE TABLE probe.t (id int PRIMARY KEY, name text)";

	private static final String INSERT = "INSERT INTO probe.t (id, name) VALUES (1, 'Widget')";

	private static final String SELECT = "SELECT name FROM probe.t WHERE id = 1";

	private static final int WARM_SAMPLES = 100;

	private WireStartupProbe() {
	}

	public static void main(final String[] args) {
		final var beforeBuild = System.nanoTime();
		final var session = SeaStarCqlSession.builder().build();
		Metrics.millis("seastar.build", System.nanoTime() - beforeBuild);

		final var beforeStart = System.nanoTime();
		final var server = SeaStarProtocolServer.builder().session(session).build().start();
		Metrics.millis("server.start", System.nanoTime() - beforeStart);
		Metrics.millis("jvm.to.listening", uptimeNanos());

		final var beforeConnect = System.nanoTime();
		final var driver = connect(server.port());
		Metrics.millis("driver.connect.cold", System.nanoTime() - beforeConnect);

		final var beforeQuery = System.nanoTime();
		driver.execute(CREATE_KEYSPACE);
		Metrics.millis("query.first", System.nanoTime() - beforeQuery);
		Metrics.millis("jvm.to.first.query", uptimeNanos());

		driver.execute(CREATE_TABLE);
		driver.execute(INSERT);

		final var beforeWarmConnect = System.nanoTime();
		final var second = connect(server.port());
		Metrics.millis("driver.connect.warm", System.nanoTime() - beforeWarmConnect);
		second.close();

		Metrics.millis("select.wire", median(times(driver)));
		Metrics.millis("select.inProcess", median(times(session)));

		final var prepared = driver.prepare(SELECT);
		final List<Long> boundTimes = new ArrayList<>();
		for (int i = 0; i < WARM_SAMPLES; i++) {
			final var before = System.nanoTime();
			driver.execute(prepared.bind());
			boundTimes.add(System.nanoTime() - before);
		}
		Metrics.millis("select.wire.prepared", median(boundTimes));

		driver.close();
		server.close();
		session.close();
	}

	/**
	 * The schema debounce window is shortened for the same reason the wire fidelity suite shortens
	 * it: the driver holds a DDL statement's answer until the metadata refresh it triggered has
	 * finished, and debounces that refresh by a second. Left alone, {@code query.first} below would
	 * be a measurement of that window and of nothing else. Everything else is stock - no pinned
	 * protocol version, no metadata switched off - so the connect figure is what an unconfigured
	 * driver actually pays.
	 *
	 * @param port the listener's port
	 * @return a session connected to it
	 */
	private static CqlSession connect(final int port) {
		final var config = DriverConfigLoader.programmaticBuilder()
			.withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofMillis(1))
			.build();

		return CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), port))
			.withLocalDatacenter("datacenter1")
			.withConfigLoader(config)
			.build();
	}

	private static List<Long> times(final CqlSession target) {
		final List<Long> times = new ArrayList<>();
		for (int i = 0; i < WARM_SAMPLES; i++) {
			final var before = System.nanoTime();
			target.execute(SELECT);
			times.add(System.nanoTime() - before);
		}

		return times;
	}

	private static long uptimeNanos() {
		return ManagementFactory.getRuntimeMXBean().getUptime() * 1_000_000L;
	}

	private static long median(final List<Long> values) {
		final var sorted = values.stream().sorted().toList();

		return sorted.get(sorted.size() / 2);
	}

}
