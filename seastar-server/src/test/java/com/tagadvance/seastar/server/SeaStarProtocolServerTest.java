package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tagadvance.seastar.SeaStarCqlSession;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * b_plan B5: the lifecycle and the builder.
 */
class SeaStarProtocolServerTest {

	private SeaStarCqlSession session;

	@BeforeEach
	void setUp() {
		session = SeaStarCqlSession.builder().build();
	}

	@AfterEach
	void tearDown() {
		session.close();
	}

	@Test
	@DisplayName("the default port is ephemeral and the bound one is readable after start")
	void testEphemeralPort() {
		try (final var server = SeaStarProtocolServer.builder().session(session).build().start()) {
			assertNotEquals(0, server.port());
			assertTrue(server.port() <= 0xFFFF);
		}
	}

	@Test
	@DisplayName("a requested port is the port that gets bound")
	void testRequestedPort() throws IOException {
		final int port;
		try (final var probe = new ServerSocket(0)) {
			port = probe.getLocalPort();
		}

		try (final var server = SeaStarProtocolServer.builder()
			.session(session)
			.port(port)
			.build()
			.start()) {
			assertEquals(port, server.port());
		}
	}

	@Test
	@DisplayName("the default bind address is loopback, and the socket accepts there")
	void testLoopbackDefault() throws IOException {
		try (final var server = SeaStarProtocolServer.builder().session(session).build().start()) {
			assertEquals(InetAddress.getLoopbackAddress(), server.bindAddress());
			try (final var socket = new Socket(InetAddress.getLoopbackAddress(), server.port())) {
				assertTrue(socket.isConnected());
			}
		}
	}

	@Test
	@DisplayName("close is idempotent")
	void testCloseIsIdempotent() {
		final var server = SeaStarProtocolServer.builder().session(session).build().start();
		server.close();
		server.close();
		server.close();
	}

	@Test
	@DisplayName("close leaves the wrapped session open, because it belongs to the caller")
	void testCloseLeavesTheSessionOpen() {
		try (final var server = SeaStarProtocolServer.builder().session(session).build().start()) {
			assertTrue(server.port() > 0);
		}

		// A closed session would throw here instead of answering.
		session.execute("CREATE KEYSPACE shop WITH replication = "
			+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
		assertTrue(session.getMetadata().getKeyspace("shop").isPresent());
	}

	@Test
	@DisplayName("a server that was never started refuses to report a port")
	void testPortBeforeStart() {
		final var server = SeaStarProtocolServer.builder().session(session).build();
		assertThrows(IllegalStateException.class, server::port);
		server.close();
	}

	@Test
	@DisplayName("a server refuses to start twice")
	void testStartTwice() {
		try (final var server = SeaStarProtocolServer.builder().session(session).build().start()) {
			assertThrows(IllegalStateException.class, server::start);
		}
	}

	@Test
	@DisplayName("a builder without a session refuses to build")
	void testSessionIsRequired() {
		assertThrows(NullPointerException.class,
			() -> SeaStarProtocolServer.builder().build());
	}

	@Test
	@DisplayName("a port outside the legal range is rejected by the builder")
	void testPortRange() {
		assertThrows(IllegalArgumentException.class,
			() -> SeaStarProtocolServer.builder().port(-1));
		assertThrows(IllegalArgumentException.class,
			() -> SeaStarProtocolServer.builder().port(0x1_0000));
	}

	@Test
	@DisplayName("a port already in use fails the start rather than the first connection")
	void testBindFailure() throws IOException {
		try (final var taken = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
			final var server = SeaStarProtocolServer.builder()
				.session(session)
				.port(taken.getLocalPort())
				.build();

			assertThrows(IllegalStateException.class, server::start);
			server.close();
		}
	}

	@Test
	@DisplayName("a bound port is released again by close")
	void testPortIsReleased() throws IOException {
		final int port;
		try (final var server = SeaStarProtocolServer.builder().session(session).build().start()) {
			port = server.port();
		}

		try (final var rebound = new ServerSocket(port, 0, InetAddress.getLoopbackAddress())) {
			assertEquals(port, rebound.getLocalPort());
		}
	}
}
