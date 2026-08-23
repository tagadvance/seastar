package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a CQL script into its individual statements on top-level semicolons, ignoring semicolons
 * that appear inside single-quoted strings, double-quoted identifiers, {@code $$}-quoted bodies
 * (the form a {@code DESCRIBE} dump writes a function body in), line comments ({@code --} or
 * {@code //}) and block comments ({@code /* ... *}{@code /}). Comments are replaced with a single
 * space so adjacent tokens do not merge; leading and trailing whitespace is trimmed from each
 * statement and empty statements are dropped.
 */
final class CqlStatements {

	private CqlStatements() {
	}

	static List<String> split(final String cql) {
		requireNonNull(cql, "cql must not be null");

		final var statements = new ArrayList<String>();
		final var current = new StringBuilder();
		final int length = cql.length();
		for (int i = 0; i < length; i++) {
			final char c = cql.charAt(i);
			switch (c) {
				case '\'', '"' -> {
					current.append(c);
					i = consumeQuoted(cql, i, c, current);
				}
				case '-' -> {
					if (i + 1 < length && cql.charAt(i + 1) == '-') {
						i = consumeLineComment(cql, i, current);
					} else {
						current.append(c);
					}
				}
				case '/' -> {
					if (i + 1 < length && cql.charAt(i + 1) == '/') {
						i = consumeLineComment(cql, i, current);
					} else if (i + 1 < length && cql.charAt(i + 1) == '*') {
						i = consumeBlockComment(cql, i, current);
					} else {
						current.append(c);
					}
				}
				case '$' -> {
					if (i + 1 < length && cql.charAt(i + 1) == '$') {
						i = consumeDollarQuoted(cql, i, current);
					} else {
						current.append(c);
					}
				}
				case ';' -> {
					addStatement(statements, current);
					current.setLength(0);
				}
				default -> current.append(c);
			}
		}
		addStatement(statements, current);

		return List.copyOf(statements);
	}

	private static void addStatement(final List<String> statements, final StringBuilder current) {
		final var statement = current.toString().trim();
		if (!statement.isEmpty()) {
			statements.add(statement);
		}
	}

	/**
	 * Consumes a quoted run starting at the opening {@code quote} (already appended), handling the
	 * doubled-quote escape ({@code ''} or {@code ""}). Returns the index of the closing quote, so
	 * the caller's loop advances to the following character; an unterminated run consumes the rest.
	 */
	private static int consumeQuoted(final String cql, final int i, final char quote,
		final StringBuilder current) {
		int j = i + 1;
		while (j < cql.length()) {
			final char c = cql.charAt(j);
			current.append(c);
			if (c == quote) {
				if (j + 1 < cql.length() && cql.charAt(j + 1) == quote) {
					current.append(quote);
					j += 2;
					continue;
				}

				return j;
			}
			j++;
		}

		return cql.length() - 1;
	}

	/**
	 * Consumes a {@code $$ ... $$} run starting at the first {@code $}. CQL has no tagged variant
	 * ({@code $tag$}), so the body runs to the next {@code $$} and nothing inside it - quotes,
	 * comments, semicolons - is interpreted. Returns the index of the closing run's last character;
	 * an unterminated body consumes the rest.
	 */
	private static int consumeDollarQuoted(final String cql, final int i,
		final StringBuilder current) {
		final int end = cql.indexOf("$$", i + 2);
		if (end < 0) {
			current.append(cql, i, cql.length());

			return cql.length() - 1;
		}
		current.append(cql, i, end + 2);

		return end + 1;
	}

	private static int consumeLineComment(final String cql, final int i,
		final StringBuilder current) {
		int j = i + 2;
		while (j < cql.length() && cql.charAt(j) != '\n') {
			j++;
		}
		current.append(' ');
		// j is the newline (re-processed as whitespace) or the end of input.
		return j - 1;
	}

	private static int consumeBlockComment(final String cql, final int i,
		final StringBuilder current) {
		final int end = cql.indexOf("*/", i + 2);
		current.append(' ');

		return end < 0 ? cql.length() - 1 : end + 1;
	}

}
