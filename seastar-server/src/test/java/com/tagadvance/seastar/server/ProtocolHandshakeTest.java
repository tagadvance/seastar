package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * b_plan B3, B4 and B7, driven straight off the wire with the driver's own client codecs, so that
 * what is asserted is the bytes rather than an in-process call.
 */
class ProtocolHandshakeTest {

	private static final int V4 = ProtocolConstants.Version.V4;

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
	@DisplayName("OPTIONS is answered with the CQL version, no compression, and v4 only")
	void testOptions() throws IOException {
		try (final var client = new WireClient(server.port())) {
			final var response = client.send(V4, 1, Options.INSTANCE);
			final var supported = assertInstanceOf(Supported.class, response.message);

			assertEquals(List.of("3.4.7"), supported.options.get(Startup.CQL_VERSION_KEY));
			assertEquals(List.of(), supported.options.get(Startup.COMPRESSION_KEY));
			assertEquals(List.of("4/v4"), supported.options.get("PROTOCOL_VERSIONS"));
		}
	}

	@Test
	@DisplayName("STARTUP is answered with READY, never with AUTHENTICATE")
	void testStartup() throws IOException {
		try (final var client = new WireClient(server.port())) {
			final var response = client.send(V4, 1, new Startup());
			assertInstanceOf(Ready.class, response.message);
		}
	}

	@Test
	@DisplayName("STARTUP asking for a compression we do not have is a PROTOCOL_ERROR naming it")
	void testStartupWithCompression() throws IOException {
		try (final var client = new WireClient(server.port())) {
			final var response = client.send(V4, 1, new Startup("lz4"));
			final var error = assertInstanceOf(Error.class, response.message);

			assertEquals(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, error.code);
			assertTrue(error.message.contains("lz4"), error.message);
		}
	}

	@Test
	@DisplayName("REGISTER is answered with READY")
	void testRegister() throws IOException {
		try (final var client = new WireClient(server.port())) {
			client.send(V4, 1, new Startup());
			final var response = client.send(V4, 2,
				new Register(List.of(ProtocolConstants.EventType.SCHEMA_CHANGE)));
			assertInstanceOf(Ready.class, response.message);
		}
	}

	@Test
	@DisplayName("an unprompted AUTH_RESPONSE is answered with AUTH_SUCCESS")
	void testAuthResponse() throws IOException {
		try (final var client = new WireClient(server.port())) {
			client.send(V4, 1, new Startup());
			final var response = client.send(V4, 2, new AuthResponse(null));
			final var success = assertInstanceOf(AuthSuccess.class, response.message);
			assertNull(success.token);
		}
	}

	@Test
	@DisplayName("the request's stream id comes back on the response")
	void testStreamIdIsEchoed() throws IOException {
		try (final var client = new WireClient(server.port())) {
			for (final int streamId : new int[] {0, 1, 42, 0x7FFF}) {
				assertEquals(streamId, client.send(V4, streamId, Options.INSTANCE).streamId);
			}
		}
	}

	@Test
	@DisplayName("a v5 STARTUP is refused in the exact shape that makes a driver downgrade")
	void testV5IsRejected() throws IOException {
		try (final var client = new WireClient(server.port())) {
			final var response = client.send(ProtocolConstants.Version.V5, 1, new Startup());
			final var error = assertInstanceOf(Error.class, response.message);

			// All three conditions ProtocolInitHandler requires before ChannelFactory will retry a
			// version lower: the code, the substring, and a response the client can still decode.
			assertEquals(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, error.code);
			assertTrue(error.message.contains("Invalid or unsupported protocol version"),
				error.message);
			assertEquals(V4, response.protocolVersion);
		}
	}

	@Test
	@DisplayName("a v3 OPTIONS is refused the same way, since only v4 is implemented")
	void testV3IsRejected() throws IOException {
		try (final var client = new WireClient(server.port())) {
			final var response = client.send(ProtocolConstants.Version.V3, 1, Options.INSTANCE);
			final var error = assertInstanceOf(Error.class, response.message);

			assertEquals(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, error.code);
			assertTrue(error.message.contains("Invalid or unsupported protocol version"),
				error.message);
		}
	}

	@Test
	@DisplayName("a statement is refused by name rather than dropped, pending result encoding")
	void testStatementsAreNotAnsweredYet() throws IOException {
		try (final var client = new WireClient(server.port())) {
			client.send(V4, 1, new Startup());
			final var response = client.send(V4, 2, new Query("SELECT * FROM system.local"));
			final var error = assertInstanceOf(Error.class, response.message);

			assertEquals(ProtocolConstants.ErrorCode.SERVER_ERROR, error.code);
			assertTrue(error.message.contains("SELECT * FROM system.local"), error.message);
		}
	}

	@Test
	@DisplayName("several connections are served independently")
	void testConcurrentConnections() throws IOException {
		try (final var first = new WireClient(server.port());
			final var second = new WireClient(server.port())) {
			assertInstanceOf(Ready.class, first.send(V4, 1, new Startup()).message);
			assertInstanceOf(Ready.class, second.send(V4, 1, new Startup()).message);
			assertInstanceOf(Supported.class, first.send(V4, 2, Options.INSTANCE).message);
		}
	}

	@Test
	@DisplayName("a tracing flag on a request does not fabricate a tracing id on the response")
	void testTracingIsNotFabricated() throws IOException {
		try (final var client = new WireClient(server.port())) {
			final var response = client.send(V4, 1, true, new Startup());

			assertNull(response.tracingId);
			assertEquals(List.of(), response.warnings);
		}
	}
}
