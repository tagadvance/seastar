package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
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
 * <p>Every test here builds a session, which is the assertion: a driver does not just open a socket
 * and start sending CQL. It negotiates a protocol version, reads the cluster name out of
 * {@code system.local}, and then refreshes its node list from {@code system.local} and
 * {@code system.peers_v2} before {@code build()} returns. Any of those going wrong is a connection
 * timeout with nothing useful in it, so reaching a built session is proof of all of them.
 *
 * <p>{@link DriverSessionTest} is where the session then does some work.
 */
@NullMarked
class DriverHandshakeTest {

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
	@DisplayName("a driver pinned to v4 completes the handshake and opens a session")
	void testPinnedToV4() {
		try (final var connected = connect(
			builder -> builder.withString(DefaultDriverOption.PROTOCOL_VERSION, "V4"))) {
			assertEquals("SeaStar", connected.getMetadata().getClusterName().orElseThrow(
				() -> new AssertionError("the cluster name comes from system.local")));
		}
	}

	@Test
	@DisplayName("a driver left on its v5 default downgrades to v4 and opens a session")
	void testDowngradeFromTheV5Default() {
		// An unconfigured driver's first byte on the wire is 66 (DSE_V2), not 5, and it walks down
		// DSE_V2 -> DSE_V1 -> V5 -> V4. Had the refusal been the wrong shape it would have stopped at
		// the first one with an UnsupportedProtocolVersionException, or - the outcome b_plan B4 is
		// really guarding against - waited out its init timeout with nothing useful to say.
		try (final var connected = connect(UnaryOperator.identity())) {
			assertEquals("V4", connected.getContext().getProtocolVersion().name());
		}
	}

	@Test
	@DisplayName("the node the driver discovers is the one system.local describes")
	void testNodeIdentity() {
		try (final var connected = connect(UnaryOperator.identity())) {
			final var nodes = connected.getMetadata().getNodes().values();

			assertEquals(1, nodes.size());
			final Node node = nodes.iterator().next();
			assertEquals("datacenter1", node.getDatacenter());
			assertEquals("rack1", node.getRack());
			assertEquals("5.0.8", String.valueOf(node.getCassandraVersion()));
			assertNotNull(node.getHostId());
			assertNotNull(node.getSchemaVersion());
			assertEquals(server.port(),
				node.getBroadcastRpcAddress().orElseThrow().getPort());
		}
	}

	@Test
	@DisplayName("a datacenter the server does not report leaves the driver with no node to use")
	void testDatacenterMismatch() {
		// The classic misconfiguration, and the reason the datacenter is on the builder at all. Note
		// where it surfaces: the session still builds, because the control connection uses the
		// contact point whatever its datacenter. The load balancing policy then marks the only node
		// there is as IGNORED, opens no pool to it, and the first statement fails saying only that no
		// node was available - with nothing about datacenters anywhere in the message.
		try (final var connected = connect("somewhere-else", UnaryOperator.identity())) {
			assertThrows(NoNodeAvailableException.class,
				() -> connected.execute("SELECT * FROM system.local"));
		}
	}

	private CqlSession connect(
		final UnaryOperator<ProgrammaticDriverConfigLoaderBuilder> configuration) {
		return connect("datacenter1", configuration);
	}

	/**
	 * Opens a session with schema metadata switched off, which is the v1 target: the control
	 * connection needs {@code system.local} and the two peers tables, and nothing else.
	 */
	private CqlSession connect(final String localDatacenter,
		final UnaryOperator<ProgrammaticDriverConfigLoaderBuilder> configuration) {
		final var loader = configuration.apply(DriverConfigLoader.programmaticBuilder()
				.withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
				.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT,
					Duration.ofSeconds(10)))
			.build();

		return CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()))
			.withLocalDatacenter(localDatacenter)
			.withConfigLoader(loader)
			.build();
	}
}
