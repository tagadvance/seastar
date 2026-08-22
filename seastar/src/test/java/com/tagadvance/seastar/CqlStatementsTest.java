package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CqlStatementsTest {

	@Test
	@DisplayName("Splits on top-level semicolons and trims each statement")
	void testSplitsTopLevelStatements() {
		assertEquals(
			List.of("CREATE TABLE a (id int PRIMARY KEY)", "CREATE TABLE b (id int PRIMARY KEY)"),
			CqlStatements.split(
				"CREATE TABLE a (id int PRIMARY KEY);\n  CREATE TABLE b (id int PRIMARY KEY);\n"));
	}

	@Test
	@DisplayName("Ignores semicolons inside single-quoted strings")
	void testIgnoresSemicolonsInStrings() {
		assertEquals(List.of("INSERT INTO t (v) VALUES ('a;b')"),
			CqlStatements.split("INSERT INTO t (v) VALUES ('a;b');"));
	}

	@Test
	@DisplayName("Handles doubled-quote escapes inside strings and identifiers")
	void testHandlesDoubledQuoteEscapes() {
		assertEquals(List.of("INSERT INTO t (v) VALUES ('it''s; ok')"),
			CqlStatements.split("INSERT INTO t (v) VALUES ('it''s; ok');"));
		assertEquals(List.of("SELECT \"a;\"\"b\" FROM t"),
			CqlStatements.split("SELECT \"a;\"\"b\" FROM t;"));
	}

	@Test
	@DisplayName("Strips line and block comments without merging adjacent tokens")
	void testStripsComments() {
		final var statements = CqlStatements.split("""
			CREATE -- inline comment; not a boundary
			TABLE a (id int PRIMARY KEY); // trailing
			/* block; comment */ CREATE TABLE b (id int PRIMARY KEY);""");
		assertEquals(2, statements.size());
		// Comments collapse to whitespace, so tokens stay separated but spacing may vary.
		assertEquals("CREATE TABLE a (id int PRIMARY KEY)", normalizeWhitespace(statements.get(0)));
		assertEquals("CREATE TABLE b (id int PRIMARY KEY)", normalizeWhitespace(statements.get(1)));
	}

	private static String normalizeWhitespace(final String statement) {
		return statement.replaceAll("\\s+", " ").trim();
	}

	@Test
	@DisplayName("Drops empty and whitespace-only statements")
	void testDropsEmptyStatements() {
		assertEquals(List.of("SELECT 1"), CqlStatements.split(";;  \n; SELECT 1 ;;"));
		assertEquals(List.of(), CqlStatements.split("  \n ; ; "));
	}

	@Test
	@DisplayName("Keeps a trailing statement with no terminating semicolon")
	void testTrailingStatementWithoutSemicolon() {
		assertEquals(List.of("SELECT 1"), CqlStatements.split("SELECT 1"));
	}

	@Test
	@DisplayName("Ignores everything inside a $$-quoted body, as a function dump writes one")
	void testDollarQuotedBody() {
		final var cql = "CREATE FUNCTION f(a int) RETURNS NULL ON NULL INPUT RETURNS int "
			+ "LANGUAGE java AS $$ return a; -- not a comment; 'not a string'; /* still body */ $$;"
			+ "SELECT 1;";
		assertEquals(List.of(
				"CREATE FUNCTION f(a int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java "
					+ "AS $$ return a; -- not a comment; 'not a string'; /* still body */ $$",
				"SELECT 1"),
			CqlStatements.split(cql));
	}

	@Test
	@DisplayName("An unterminated $$ body runs to the end of the script")
	void testUnterminatedDollarQuotedBody() {
		assertEquals(List.of("CREATE FUNCTION f AS $$ return 1;"),
			CqlStatements.split("CREATE FUNCTION f AS $$ return 1;"));
	}

	@Test
	@DisplayName("A lone $ is an ordinary character")
	void testLoneDollar() {
		assertEquals(List.of("INSERT INTO t (v) VALUES ('$5')", "SELECT 1"),
			CqlStatements.split("INSERT INTO t (v) VALUES ('$5'); SELECT 1;"));
	}

}
