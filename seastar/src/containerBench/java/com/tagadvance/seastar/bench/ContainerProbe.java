package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The TestContainers baseline that SeaStar is measured against. This is not a microbenchmark - it is
 * a stopwatch around {@code container.start()} and the first successful query - and it needs Docker,
 * so it is opt-in and lives behind its own Gradle task.
 *
 * <p>Usage: {@code ContainerProbe [warm|cold|memory] [rows]}. {@code cold} removes every local tag of
 * the image first so that the pull is measured too, then restores the tags it removed. {@code memory}
 * boots a warm image, seeds the fixture schema, loads {@code rows} rows and reports heap/RSS on both
 * sides of the socket.
 */
public final class ContainerProbe {

	private static final int PORT = 9042;

	/**
	 * Pinned to the same version as the {@code cassandra-all} dependency and the container test
	 * suite; bump all three together.
	 */
	private static final String IMAGE = "cassandra:5.0.8";

	private static final String FIRST_QUERY =
		"CREATE KEYSPACE probe WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";

	private static final int ASYNC_WINDOW = 200;

	private ContainerProbe() {
	}

	public static void main(final String[] args) throws IOException, InterruptedException {
		// First statement of main, matching the M1 convention every other probe uses - see
		// StartupProbe. Kept even though this probe's own headline clock starts at
		// container.start(), so the table can carry one column of like-for-like numbers.
		final var mainStart = System.nanoTime();

		final var mode = args.length > 0 ? args[0] : "warm";
		if ("memory".equals(mode)) {
			memory(mainStart, args.length > 1 ? Integer.parseInt(args[1]) : 0);
			return;
		}

		final var tags = switch (mode) {
			case "warm" -> List.<String>of();
			case "cold" -> removeImage();
			default -> throw new IllegalArgumentException(
				"mode must be warm, cold or memory but was " + mode);
		};

		if (!tags.isEmpty()) {
			final var beforePull = System.nanoTime();
			docker("pull", IMAGE);
			Metrics.millis("image.pull", System.nanoTime() - beforePull);
		}

		try {
			measure(mainStart);
		} finally {
			tags.stream().filter(tag -> !IMAGE.equals(tag))
				.forEach(tag -> quietly(() -> docker("tag", IMAGE, tag)));
		}
	}

	private static void measure(final long mainStart) throws IOException, InterruptedException {
		final var container = new CassandraContainer(DockerImageName.parse(IMAGE));

		final var beforeStart = System.nanoTime();
		container.start();
		final var afterStart = System.nanoTime();
		Metrics.millis("container.start", afterStart - beforeStart);

		try (final var session = CqlSession.builder()
			.addContactPoint(new InetSocketAddress(container.getHost(), container.getMappedPort(PORT)))
			.withLocalDatacenter(container.getLocalDatacenter())
			.build()) {
			final var afterBuild = System.nanoTime();
			Metrics.millis("session.build", afterBuild - afterStart);

			session.execute(FIRST_QUERY);
			final var afterQuery = System.nanoTime();
			Metrics.millis("query.first", afterQuery - afterBuild);
			Metrics.millis("start.to.first.query", afterQuery - beforeStart);
			Metrics.millis("main.to.first.query", afterQuery - mainStart);
		} finally {
			container.stop();
		}
	}

	/**
	 * Boots a warm image, seeds the 75-statement fixture schema, loads {@code rows} rows into one
	 * extra table, then reports memory on both sides of the socket: the driver-side JVM's heap and
	 * RSS (M2a/b), the container's RSS as {@code docker stats} sees it, and the heap the node's own
	 * {@code cassandra-env.sh} sized itself to, via {@code nodetool info}.
	 */
	private static void memory(final long mainStart, final int rows)
		throws IOException, InterruptedException {
		final var rssBefore = readRssKb();
		gcSettle();
		final var heapBefore = heapUsedBytes();

		final var container = new CassandraContainer(DockerImageName.parse(IMAGE));
		container.start();
		// Shortened the same way the turnaround benchmarks are: left at its one second default, the
		// 75-statement schema replay below would mostly measure the debounce window.
		final var config = DriverConfigLoader.programmaticBuilder()
			.withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofMillis(1))
			.build();
		try (final var session = CqlSession.builder()
			.addContactPoint(new InetSocketAddress(container.getHost(), container.getMappedPort(PORT)))
			.withLocalDatacenter(container.getLocalDatacenter())
			.withConfigLoader(config)
			.build()) {
			for (final var statement : BenchmarkSchema.cql().split(";\\s*\n")) {
				final var trimmed = statement.trim();
				if (!trimmed.isEmpty()) {
					session.execute(trimmed);
				}
			}
			session.execute(FIRST_QUERY);
			session.execute("CREATE TABLE probe.mem (id int PRIMARY KEY, name text, age int)");

			if (rows > 0) {
				final var prepared = session.prepare(
					"INSERT INTO probe.mem (id, name, age) VALUES (?, ?, ?)");
				final var inFlight = new ArrayDeque<CompletionStage<AsyncResultSet>>();
				for (int id = 0; id < rows; id++) {
					inFlight.add(session.executeAsync(prepared.bind(id, "name-" + id, id % 100)));
					if (inFlight.size() >= ASYNC_WINDOW) {
						inFlight.poll().toCompletableFuture().join();
					}
				}
				while (!inFlight.isEmpty()) {
					inFlight.poll().toCompletableFuture().join();
				}
			}

			gcSettle();
			final var heapAfter = heapUsedBytes();
			final var rssAfter = readRssKb();

			Metrics.count("memory.rows", rows);
			Metrics.value("memory.driver.heap.used.mb", (heapAfter - heapBefore) / (1024.0d * 1024.0d));
			Metrics.value("memory.driver.rss.kb", rssAfter);
			Metrics.value("memory.driver.rss.delta.kb", rssAfter - rssBefore);

			final var containerId = container.getContainerId();
			Metrics.value("memory.container.rss.kb", dockerStatsRssKb(containerId));
			Metrics.value("memory.container.heap.reserved.mb", nodetoolHeapMb(containerId));
		} finally {
			container.stop();
		}
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
	 * Linux only - reads VmRSS out of {@code /proc/self/status}.
	 */
	private static long readRssKb() throws IOException {
		return Files.readAllLines(Path.of("/proc/self/status")).stream()
			.filter(line -> line.startsWith("VmRSS:"))
			.findFirst()
			.map(line -> Long.parseLong(line.replaceAll("[^0-9]", "")))
			.orElse(-1L);
	}

	/**
	 * {@code docker stats} reports e.g. {@code "812.3MiB / 62GiB"}; only the used side is wanted.
	 */
	private static double dockerStatsRssKb(final String containerId)
		throws IOException, InterruptedException {
		final var output = docker("stats", "--no-stream", "--format", "{{.MemUsage}}", containerId)
			.trim();
		final var used = output.split("/")[0].trim();

		return parseMemToKb(used);
	}

	private static double parseMemToKb(final String value) {
		final var number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
		if (value.contains("GiB")) {
			return number * 1024 * 1024;
		} else if (value.contains("MiB")) {
			return number * 1024;
		} else if (value.contains("KiB")) {
			return number;
		}

		return number / 1024;
	}

	/**
	 * {@code nodetool info}'s "Heap Memory (MB)" line reports {@code used / committed}; the
	 * committed side is what {@code cassandra-env.sh} sized the node's max heap to.
	 */
	private static double nodetoolHeapMb(final String containerId)
		throws IOException, InterruptedException {
		final var command = new ArrayList<String>(
			List.of("exec", containerId, "nodetool", "info"));
		final var output = docker(command.toArray(String[]::new));

		return output.lines()
			.filter(line -> line.contains("Heap Memory"))
			.findFirst()
			.map(line -> line.substring(line.indexOf(':') + 1).trim().split("/")[1].trim())
			.map(Double::parseDouble)
			.orElse(-1.0d);
	}

	/**
	 * Removes every local tag that resolves to the benchmark image, so the following pull is a real
	 * one rather than a no-op against layers that are still cached.
	 *
	 * @return the tags that were removed, so they can be restored afterwards
	 */
	private static List<String> removeImage() throws IOException, InterruptedException {
		final var id = docker("image", "inspect", "--format", "{{.Id}}", IMAGE).trim();
		if (id.isEmpty()) {
			return List.of(IMAGE);
		}

		final var listed = docker("image", "ls", "--no-trunc", "--format", "{{.ID}} {{.Repository}}:{{.Tag}}");
		final var tags = listed.lines()
			.map(line -> line.split(" ", 2))
			.filter(parts -> parts.length == 2 && parts[0].equals(id))
			.map(parts -> parts[1])
			.filter(tag -> !tag.endsWith(":<none>"))
			.toList();

		final var command = new ArrayList<String>(List.of("image", "rm", "-f"));
		command.addAll(tags.isEmpty() ? List.of(IMAGE) : tags);
		docker(command.toArray(String[]::new));

		return tags.isEmpty() ? List.of(IMAGE) : tags;
	}

	private static String docker(final String... args) throws IOException, InterruptedException {
		final var command = new ArrayList<String>(List.of("docker"));
		command.addAll(Arrays.asList(args));

		final var process = new ProcessBuilder(command).redirectErrorStream(true).start();
		final var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		final var status = process.waitFor();
		if (status != 0) {
			throw new IllegalStateException(
				"%s exited %d: %s".formatted(String.join(" ", command), status, output));
		}

		return output;
	}

	private static void quietly(final DockerCommand command) {
		try {
			command.run();
		} catch (final IOException | InterruptedException | RuntimeException e) {
			System.err.println("failed to restore image tag: " + e.getMessage());
		}
	}

	@FunctionalInterface
	private interface DockerCommand {

		void run() throws IOException, InterruptedException;

	}

}
