package com.tagadvance.seastar.bench;

/**
 * The wire format between a cold-JVM probe and {@link ColdJvmBenchmark}. A probe prints one
 * {@code METRIC<tab>name<tab>value} line per measurement; anything else it writes to stdout is
 * ignored, so probes stay readable when run by hand.
 */
final class Metrics {

	static final String PREFIX = "METRIC";

	private Metrics() {
	}

	/**
	 * Reports an elapsed time, converted from nanoseconds to milliseconds.
	 */
	static void millis(final String name, final long nanos) {
		value(name, nanos / 1_000_000.0d);
	}

	/**
	 * Reports a dimensionless count.
	 */
	static void count(final String name, final long count) {
		value(name, count);
	}

	static void value(final String name, final double value) {
		System.out.printf("%s\t%s\t%s%n", PREFIX, name, value);
	}

}
