package com.tagadvance.seastar.bench;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A synthetic but realistically shaped schema: five keyspaces, ten tables each, with user defined
 * types and secondary indexes. This is the shape a real test fixture seeds through
 * {@code withSchema}, and it is where per-statement cost turns into startup cost.
 */
final class BenchmarkSchema {

	static final int KEYSPACES = 5;
	static final int TABLES_PER_KEYSPACE = 10;
	static final int TYPES_PER_KEYSPACE = 2;
	static final int INDEXES_PER_KEYSPACE = 2;

	private BenchmarkSchema() {
	}

	/**
	 * @return the whole schema as one semicolon separated CQL script
	 */
	static String cql() {
		return IntStream.range(0, KEYSPACES)
			.mapToObj(BenchmarkSchema::keyspace)
			.collect(Collectors.joining("\n"));
	}

	/**
	 * @return the number of statements {@link #cql()} expands to
	 */
	static int statementCount() {
		return KEYSPACES * (1 + TYPES_PER_KEYSPACE + TABLES_PER_KEYSPACE + INDEXES_PER_KEYSPACE);
	}

	private static String keyspace(final int index) {
		final var name = "bench_ks_" + index;
		final var builder = new StringBuilder();
		builder.append("CREATE KEYSPACE ").append(name)
			.append(" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };\n");
		builder.append("CREATE TYPE ").append(name)
			.append(".address (street text, city text, zip int);\n");
		builder.append("CREATE TYPE ").append(name)
			.append(".contact (email text, phone text);\n");

		IntStream.range(0, TABLES_PER_KEYSPACE)
			.mapToObj(table -> table(name, table))
			.forEach(builder::append);

		builder.append("CREATE INDEX ON ").append(name).append(".table_0 (label);\n");
		builder.append("CREATE INDEX ON ").append(name).append(".table_1 (label);\n");

		return builder.toString();
	}

	private static String table(final String keyspace, final int index) {
		// Every third table carries a frozen UDT column so type resolution is exercised too.
		final var udtColumn = index % 3 == 0 ? "\thome frozen<address>,\n" : "";

		return """
			CREATE TABLE %s.table_%d (
				pk uuid,
				ck timestamp,
				label text,
				amount double,
				quantity int,
				active boolean,
			%s	PRIMARY KEY (pk, ck)
			);
			""".formatted(keyspace, index, udtColumn);
	}

}
