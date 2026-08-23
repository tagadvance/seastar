package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SeaStarCqlSessionBuilderTest {

	private static final String SCHEMA = """
		CREATE KEYSPACE shop WITH replication =
			{'class': 'SimpleStrategy', 'replication_factor': 1};
		CREATE TABLE shop.products (id int PRIMARY KEY, name text);
		""";

	@Test
	@DisplayName("withSchema seeds the session from a CQL string")
	void testWithSchemaString() {
		try (final var session = SeaStarCqlSession.builder().withSchema(SCHEMA).build()) {
			session.execute("INSERT INTO shop.products (id, name) VALUES (1, 'Widget')");
			final var row = session.execute("SELECT name FROM shop.products WHERE id = 1").one();
			assertEquals("Widget", row.getString("name"));
		}
	}

	@Test
	@DisplayName("withSchemaResource seeds the session from a classpath resource")
	void testWithSchemaResource() {
		try (final var session = SeaStarCqlSession.builder()
			.withSchemaResource("schema.cql")
			.build()) {
			assertTrue(session.getMetadata()
				.getKeyspace("shop")
				.flatMap(ks -> ks.getTable("products"))
				.isPresent());
		}
	}

	@Test
	@DisplayName("withSchemaFile seeds the session from a filesystem file")
	void testWithSchemaFile(@TempDir final Path dir) throws Exception {
		final var file = dir.resolve("schema.cql");
		Files.writeString(file, SCHEMA);

		try (final var session = SeaStarCqlSession.builder().withSchemaFile(file).build()) {
			assertTrue(session.getMetadata()
				.getKeyspace("shop")
				.flatMap(ks -> ks.getTable("products"))
				.isPresent());
		}
	}

	@Test
	@DisplayName("A failing schema statement fails the build with a describing message")
	void testFailingStatementFailsBuild() {
		final var builder = SeaStarCqlSession.builder().withSchema("CREATE TABLE nope.t (id int)");
		final var e = assertThrows(IllegalStateException.class, builder::build);
		assertTrue(e.getMessage().contains("Failed to execute schema statement"), e.getMessage());
	}

	@Test
	@DisplayName("A missing schema resource fails the build with a describing message")
	void testMissingResourceFailsBuild() {
		final var builder = SeaStarCqlSession.builder().withSchemaResource("does-not-exist.cql");
		final var e = assertThrows(IllegalStateException.class, builder::build);
		assertTrue(e.getMessage().contains("Failed to read schema"), e.getMessage());
	}

	/**
	 * The shape of a {@code DESCRIBE SCHEMA} dump: features SeaStar refuses, non-CQL noise, and a
	 * statement the parser rejects, mixed in with statements that work. Lenient keeps everything it
	 * can; strict fails on the first refusal.
	 */
	@Test
	@DisplayName("A lenient import skips what fails and seeds everything else")
	void testLenientImportSkipsFailures() {
		final var dump = SCHEMA + """
			Warnings :
			CREATE MATERIALIZED VIEW shop.by_name AS SELECT * FROM shop.products
				WHERE name IS NOT NULL AND id IS NOT NULL PRIMARY KEY (name, id);
			CREATE FUNCTION shop.f(a int) RETURNS NULL ON NULL INPUT RETURNS int
				LANGUAGE java AS $$ return a; $$;
			CREATE OR REPLACE FUNCTION shop.f(a int) RETURNS NULL ON NULL INPUT RETURNS int
				LANGUAGE java AS $$ return a; $$;
			CREATE AGGREGATE shop.agg(int) SFUNC f STYPE int;
			CREATE TABLE shop.orders (id int PRIMARY KEY, total int);
			""";

		assertThrows(IllegalStateException.class,
			() -> SeaStarCqlSession.builder().withSchema(dump).build());

		try (final var session = SeaStarCqlSession.builder()
			.withSchema(dump, SchemaImport.LENIENT)
			.build()) {
			final var keyspace = session.getMetadata().getKeyspace("shop").orElseThrow();
			assertTrue(keyspace.getTable("products").isPresent());
			assertTrue(keyspace.getTable("orders").isPresent());
		}
	}

	@Test
	@DisplayName("A lenient import strips the table options Cassandra removed in 4.0")
	void testLenientImportStripsRemovedOptions() {
		final var dump = """
			CREATE KEYSPACE legacy WITH replication =
				{'class': 'SimpleStrategy', 'replication_factor': 1};
			CREATE TABLE legacy.first (id int PRIMARY KEY)
				WITH read_repair_chance = 0.0 AND comment = 'kept';
			CREATE TABLE legacy.middle (id int PRIMARY KEY)
				WITH comment = 'kept' AND dclocal_read_repair_chance = 0.1 AND gc_grace_seconds = 864000;
			CREATE TABLE legacy.only (id int PRIMARY KEY) WITH read_repair_chance = 0.0;
			""";

		try (final var session = SeaStarCqlSession.builder()
			.withSchema(dump, SchemaImport.LENIENT)
			.build()) {
			final var keyspace = session.getMetadata().getKeyspace("legacy").orElseThrow();
			// The dead option is stripped rather than the statement skipped: the tables exist.
			assertTrue(keyspace.getTable("first").isPresent());
			assertTrue(keyspace.getTable("middle").isPresent());
			assertTrue(keyspace.getTable("only").isPresent());
		}
	}

	/**
	 * Transport settings a caller might set on a builder shared with a
	 * real session are accepted and ignored rather than rejected.
	 */
	@Test
	@DisplayName("Transport settings on the builder are accepted rather than rejected")
	void testTransportSettingsAccepted() {
		try (final var session = SeaStarCqlSession.builder()
			.withAuthProvider(new ProgrammaticPlainTextAuthProvider("user", "pass"))
			.withAuthCredentials("user", "pass")
			.withLocalDatacenter("dc1")
			.withMetricRegistry(new Object())
			.build()) {
			assertTrue(session.getMetadata().getKeyspaces().isEmpty());
		}
	}

	/**
	 * A contact point names a real address to connect to, which SeaStar
	 * has none of, so accepting one silently would misrepresent what got configured.
	 */
	@Test
	@DisplayName("A contact point is rejected: SeaStar has no address to connect to")
	void testContactPointRejected() {
		final var builder = SeaStarCqlSession.builder();
		assertThrows(UnsupportedOperationException.class,
			() -> builder.addContactPoint(new InetSocketAddress("localhost", 9042)));
		assertThrows(UnsupportedOperationException.class,
			() -> builder.addContactPoints(List.of(new InetSocketAddress("localhost", 9042))));
	}

}
