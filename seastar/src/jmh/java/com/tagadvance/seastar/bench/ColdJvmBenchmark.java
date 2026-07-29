package com.tagadvance.seastar.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Runs a probe class in a fresh JVM {@code N} times and reports the distribution of every metric it
 * printed.
 *
 * <p>Startup is not a steady-state measurement. Class loading is most of what is being measured, so
 * JMH's warmup would erase the very thing under test; each sample therefore has to pay the cost from
 * a cold JVM. That is the whole reason this exists instead of a {@code @Benchmark} method.
 *
 * <p>Usage: {@code ColdJvmBenchmark <probe-class> <samples> [probe args...]}. One argument may list
 * slash separated variants ({@code direct/queryProcessor}); the variants are then run round robin
 * rather than one after the other, so that a machine warming up or throttling during the run biases
 * all of them equally, and every metric is reported once per variant.
 */
public final class ColdJvmBenchmark {

	private ColdJvmBenchmark() {
	}

	public static void main(final String[] args) throws IOException, InterruptedException {
		if (args.length < 2) {
			throw new IllegalArgumentException(
				"usage: ColdJvmBenchmark <probe-class> <samples> [probe args...]");
		}
		final var probe = args[0];
		final var samples = Integer.parseInt(args[1]);
		final var probeArgs = List.of(args).subList(2, args.length);
		final var variants = variants(probeArgs);

		final Map<String, List<Double>> samplesByMetric = new LinkedHashMap<>();
		for (int i = 0; i < samples * variants.size(); i++) {
			final var variant = variants.get(i % variants.size());
			final var prefix = variants.size() == 1 ? "" : variant.get(variantIndex(probeArgs)) + '.';
			run(probe, variant).forEach(
				(name, value) -> samplesByMetric.computeIfAbsent(prefix + name,
					key -> new ArrayList<>()).add(value));
		}

		print(probe, probeArgs, samples, samplesByMetric);
	}

	private static int variantIndex(final List<String> probeArgs) {
		return IntStream.range(0, probeArgs.size())
			.filter(index -> probeArgs.get(index).contains("/"))
			.findFirst()
			.orElse(-1);
	}

	/**
	 * Expands the one argument holding slash separated variants into one argument list per variant.
	 */
	private static List<List<String>> variants(final List<String> probeArgs) {
		final var index = variantIndex(probeArgs);
		if (index < 0) {
			return List.of(probeArgs);
		}

		return Stream.of(probeArgs.get(index).split("/")).map(variant -> {
			final var expanded = new ArrayList<>(probeArgs);
			expanded.set(index, variant);

			return List.copyOf(expanded);
		}).toList();
	}

	private static Map<String, Double> run(final String probe, final List<String> probeArgs)
		throws IOException, InterruptedException {
		final var command = new ArrayList<String>();
		command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
		final var logback = System.getProperty("logback.configurationFile");
		if (logback != null) {
			command.add("-Dlogback.configurationFile=" + logback);
		}
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add(probe);
		command.addAll(probeArgs);

		final var output = Files.createTempFile("seastar-probe", ".out");
		try {
			final var process = new ProcessBuilder(command).redirectErrorStream(true)
				.redirectOutput(output.toFile())
				.start();
			final var status = process.waitFor();
			final var lines = Files.readAllLines(output);
			if (status != 0) {
				throw new IllegalStateException(
					"probe %s exited %d:%n%s".formatted(probe, status, String.join("\n", lines)));
			}

			return parse(lines);
		} finally {
			Files.deleteIfExists(output);
		}
	}

	private static Map<String, Double> parse(final List<String> lines) {
		final Map<String, Double> metrics = new LinkedHashMap<>();
		lines.stream()
			.filter(line -> line.startsWith(Metrics.PREFIX + '\t'))
			.map(line -> line.split("\t"))
			.filter(parts -> parts.length == 3)
			.forEach(parts -> metrics.put(parts[1], Double.valueOf(parts[2])));

		return metrics;
	}

	private static void print(final String probe, final List<String> probeArgs, final int samples,
		final Map<String, List<Double>> samplesByMetric) {
		System.out.printf("%n%s %s, %d cold JVMs%n", probe, String.join(" ", probeArgs), samples);
		System.out.printf("%-34s %9s %9s %9s %9s %9s%n", "metric", "min", "median", "mean", "p90",
			"max");
		samplesByMetric.forEach((name, values) -> {
			final var sorted = values.stream().sorted(Comparator.naturalOrder()).toList();
			System.out.printf("%-34s %9.2f %9.2f %9.2f %9.2f %9.2f%n", name, sorted.get(0),
				percentile(sorted, 0.5d), mean(sorted), percentile(sorted, 0.9d),
				sorted.get(sorted.size() - 1));
		});
	}

	private static double mean(final List<Double> sorted) {
		return sorted.stream().mapToDouble(Double::doubleValue).average().orElseThrow(
			() -> new IllegalStateException("a metric must have at least one sample to be reported"));
	}

	private static double percentile(final List<Double> sorted, final double quantile) {
		final var index = (int) Math.min(sorted.size() - 1L,
			Math.round(quantile * (sorted.size() - 1)));

		return sorted.get(index);
	}

}
