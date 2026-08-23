package com.tagadvance.seastar;

/**
 * How {@link SeaStarCqlSessionBuilder#withSchema(String, SchemaImport)} and its file and resource
 * variants treat a statement that fails.
 *
 * <p>{@link #STRICT} is for the schema a test owns; {@link #LENIENT} is for a schema dump taken
 * from a live cluster ({@code cqlsh -e 'DESCRIBE SCHEMA'}), which legitimately contains statements
 * SeaStar refuses - materialized views, functions, aggregates - and, from an older cluster, table
 * options that no longer exist.
 */
public enum SchemaImport {

	/**
	 * Every statement must succeed; the first failure fails the build with an
	 * {@link IllegalStateException} naming the statement. The default, and what the mode-less
	 * {@code withSchema} methods do.
	 */
	STRICT,

	/**
	 * A statement that fails is logged at WARN - statement and reason - and skipped, so a dump
	 * containing features SeaStar does not implement seeds everything it can instead of failing on
	 * the first refusal. Before a statement runs, table options that Cassandra itself has removed
	 * ({@code read_repair_chance}, {@code dclocal_read_repair_chance}) are stripped, so a pre-4.0
	 * dump keeps its tables rather than losing each {@code CREATE TABLE} to a dead option.
	 *
	 * <p>Lenient means lenient: a typo in a statement is also skipped with a warning rather than
	 * failing the build, so prefer {@link #STRICT} for CQL written by hand.
	 */
	LENIENT
}
