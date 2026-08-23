package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * TRUNCATE reset cost against a real node - the price a Testcontainers suite pays between tests,
 * once per test rather than once per suite. {@code auto_snapshot} is on by default and snapshots
 * the table to disk before truncating it, even though the container is thrown away at the end of
 * the run.
 *
 * <p>Usage: {@code TruncateProbe [true|false]} - the argument is {@code auto_snapshot}.
 */
public final class TruncateProbe {

	private static final String IMAGE = "cassandra:5.0.8";

	private static final int PORT = 9042;

	private static final int ROWS = 1_000;

	private static final int SAMPLES = 5;

	private static final int ASYNC_WINDOW = 200;

	private TruncateProbe() {
	}

	public static void main(final String[] args) throws IOException, InterruptedException {
		final var autoSnapshot = args.length == 0 || Boolean.parseBoolean(args[0]);

		final var container = new CassandraContainer(DockerImageName.parse(IMAGE));
		if (!autoSnapshot) {
			container.withCopyFileToContainer(
				MountableFile.forHostPath(patchedYaml()), "/etc/cassandra/cassandra.yaml");
		}
		container.start();

		try (final var session = CqlSession.builder()
			.addContactPoint(new InetSocketAddress(container.getHost(), container.getMappedPort(PORT)))
			.withLocalDatacenter(container.getLocalDatacenter())
			.build()) {
			session.execute("CREATE KEYSPACE probe WITH REPLICATION = "
				+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			session.execute("CREATE TABLE probe.t (id int PRIMARY KEY, name text, age int)");
			final var insert = session.prepare("INSERT INTO probe.t (id, name, age) VALUES (?, ?, ?)");

			final List<Long> samples = new ArrayList<>();
			for (int i = 0; i < SAMPLES; i++) {
				seed(session, insert);
				final var before = System.nanoTime();
				session.execute("TRUNCATE probe.t");
				samples.add(System.nanoTime() - before);
			}

			Metrics.count("auto_snapshot", autoSnapshot ? 1 : 0);
			Metrics.millis("truncate", median(samples));
		} finally {
			container.stop();
		}
	}

	private static void seed(final CqlSession session, final PreparedStatement insert) {
		final var inFlight = new ArrayDeque<CompletionStage<AsyncResultSet>>();
		for (int id = 0; id < ROWS; id++) {
			inFlight.add(session.executeAsync(insert.bind(id, "name-" + id, id % 100)));
			if (inFlight.size() >= ASYNC_WINDOW) {
				inFlight.poll().toCompletableFuture().join();
			}
		}
		while (!inFlight.isEmpty()) {
			inFlight.poll().toCompletableFuture().join();
		}
	}

	/**
	 * Extracts the image's real {@code cassandra.yaml} from a throwaway container rather than hand
	 * authoring a replacement, then flips one line. Overlaying just this file - not the whole config
	 * directory {@code withConfigurationOverride} would - keeps everything else the image ships with
	 * intact.
	 */
	private static Path patchedYaml() throws IOException, InterruptedException {
		final var probe = new CassandraContainer(DockerImageName.parse(IMAGE));
		probe.start();
		final var extracted = Files.createTempFile("cassandra", ".yaml");
		try {
			docker("cp", probe.getContainerId() + ":/etc/cassandra/cassandra.yaml",
				extracted.toString());
		} finally {
			probe.stop();
		}

		final var patched = Files.readString(extracted, StandardCharsets.UTF_8)
			.replace("auto_snapshot: true", "auto_snapshot: false");
		final var out = Files.createTempFile("cassandra-no-snapshot", ".yaml");
		Files.writeString(out, patched, StandardCharsets.UTF_8);

		return out;
	}

	private static String docker(final String... args) throws IOException, InterruptedException {
		final var command = new ArrayList<String>(List.of("docker"));
		command.addAll(List.of(args));
		final var process = new ProcessBuilder(command).redirectErrorStream(true).start();
		final var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		final var status = process.waitFor();
		if (status != 0) {
			throw new IllegalStateException(
				"%s exited %d: %s".formatted(String.join(" ", command), status, output));
		}

		return output;
	}

	private static long median(final List<Long> values) {
		final var sorted = values.stream().sorted().toList();

		return sorted.get(sorted.size() / 2);
	}

}
