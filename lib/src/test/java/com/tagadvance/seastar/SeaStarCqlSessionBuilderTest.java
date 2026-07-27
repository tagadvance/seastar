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
	 * f_plan_api_contract.txt F6: transport settings a caller might set on a builder shared with a
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
	 * f_plan_api_contract.txt F6: a contact point names a real address to connect to, which SeaStar
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
