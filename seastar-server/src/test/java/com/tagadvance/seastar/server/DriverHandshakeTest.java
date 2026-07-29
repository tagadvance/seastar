package com.tagadvance.seastar.server;

import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.function.UnaryOperator;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The handshake as a real driver sees it - no wire client, no mock, an actual {@link CqlSession}
 * opening an actual socket.
 *
 * <p>Both tests assert that the connection <em>fails at the cluster-name query</em>, which is a
 * deliberate and temporary shape. {@code ProtocolInitHandler} runs
 * {@code SELECT cluster_name FROM system.local} as the step immediately after {@code READY}, so
 * reaching that failure is proof the whole handshake succeeded, and failing anywhere earlier is
 * proof it did not. Once the system tables are answered these become plain connect-and-query
 * tests; the assertion is written to fail loudly rather than pass vacuously when that happens.
 */
@NullMarked
class DriverHandshakeTest {

	/** The first thing the driver asks for once the protocol is up. */
	private static final String FIRST_QUERY = "SELECT cluster_name FROM system.local";

	private SeaStarCqlSession session;
	private SeaStarProtocolServer server;

	@BeforeEach
	void setUp() {
		session = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(session).build().start();
	}

	@AfterEach
	void tearDown() {
		server.close();
		session.close();
	}

	@Test
	@DisplayName("a driver pinned to v4 completes the handshake and gets as far as its first query")
	void testPinnedToV4() {
		final var errors = connectAndFail(
			builder -> builder.withString(DefaultDriverOption.PROTOCOL_VERSION, "V4"));

		assertTrue(errors.contains(FIRST_QUERY), errors);
	}

	@Test
	@DisplayName("a driver left on its v5 default downgrades to v4 rather than failing or hanging")
	void testDowngradeFromTheV5Default() {
		final var errors = connectAndFail(UnaryOperator.identity());

		// Had the refusal been the wrong shape the driver would have stopped at v5 with an
		// UnsupportedProtocolVersionException, or - the outcome b_plan B4 is really guarding
		// against - waited out its init timeout with nothing useful to say. Reaching the first
		// query means it retried at v4 and the handshake succeeded there.
		assertTrue(errors.contains(FIRST_QUERY), errors);
		assertFalse(errors.contains("Invalid or unsupported protocol version"), errors);
	}

	private String connectAndFail(
		final UnaryOperator<ProgrammaticDriverConfigLoaderBuilder> configuration) {
		final var loader = configuration.apply(DriverConfigLoader.programmaticBuilder()
				.withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
				.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT,
					Duration.ofSeconds(10)))
			.build();
		final var builder = CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()))
			.withLocalDatacenter("datacenter1")
			.withConfigLoader(loader);

		final var thrown = assertThrows(AllNodesFailedException.class, builder::build);

		return thrown.getAllErrors()
			.values()
			.stream()
			.flatMap(List::stream)
			.map(String::valueOf)
			.collect(joining("\n"));
	}
}
