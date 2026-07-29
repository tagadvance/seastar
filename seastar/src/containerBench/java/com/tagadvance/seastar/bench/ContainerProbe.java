package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The TestContainers baseline that SeaStar is measured against. This is not a microbenchmark - it is
 * a stopwatch around {@code container.start()} and the first successful query - and it needs Docker,
 * so it is opt-in and lives behind its own Gradle task.
 *
 * <p>Usage: {@code ContainerProbe [warm|cold]}. {@code cold} removes every local tag of the image
 * first so that the pull is measured too, then restores the tags it removed.
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

	private ContainerProbe() {
	}

	public static void main(final String[] args) throws IOException, InterruptedException {
		final var mode = args.length > 0 ? args[0] : "warm";
		final var tags = switch (mode) {
			case "warm" -> List.<String>of();
			case "cold" -> removeImage();
			default -> throw new IllegalArgumentException("mode must be warm or cold but was " + mode);
		};

		if (!tags.isEmpty()) {
			final var beforePull = System.nanoTime();
			docker("pull", IMAGE);
			Metrics.millis("image.pull", System.nanoTime() - beforePull);
		}

		try {
			measure();
		} finally {
			tags.stream().filter(tag -> !IMAGE.equals(tag))
				.forEach(tag -> quietly(() -> docker("tag", IMAGE, tag)));
		}
	}

	private static void measure() throws IOException, InterruptedException {
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
		} finally {
			container.stop();
		}
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
