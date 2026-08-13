package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * The wire backend for the fidelity suite: the same expectations {@code SeaStar*FidelityTest} runs
 * in process and {@code Container*FidelityTest} runs against a real node, run again over a socket.
 * In-process SeaStar and wire SeaStar have to be indistinguishable through the driver API, and
 * every statement type, error path and type mapping the suite already covers becomes a protocol
 * conformance test for free. Each {@code Wire*FidelityTest} registers one of these and delegates
 * {@code createInstance()} to it.
 *
 * <p>The session is otherwise stock - contact point and datacenter, schema metadata left on - so
 * the suite also exercises the driver's own request pipeline, metadata refresh and paging
 * termination. The one thing configured is the schema debounce window; see
 * {@link #connect(SeaStarProtocolServer, String)} for why, and for what it costs to leave alone.
 *
 * <p>The suite runs over a session that negotiated its own version and settled on v5, so
 * {@code hasResultMetadataId()} is left at its default; {@code WireProtocolVersionTest} covers the
 * v4 half. No Docker, so this runs on the default {@code test} task rather than behind the
 * {@code container} tag.
 *
 * <p>An extension rather than an {@code @AfterAll} method: extension callbacks run after every
 * {@code @AfterAll}, so the driver session the suite owns is closed while the server is still
 * listening.
 */
final class WireHarness implements AfterAllCallback {

	private SeaStarCqlSession backing;

	private SeaStarProtocolServer server;

	CqlSession createInstance() {
		backing = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(backing).build().start();

		return connect(server, null);
	}

	@Override
	public void afterAll(final ExtensionContext context) {
		if (server != null) {
			server.close();
		}
		if (backing != null) {
			backing.close();
		}
	}

	/**
	 * The driver debounces its schema refresh by a second, and it holds a DDL statement's answer
	 * until the refresh it triggered has completed - so the suite, which is mostly DDL, spent a
	 * second per statement waiting for a window rather than for SeaStar. Measured: <strong>190 s
	 * with the default window, 6.9 s at 1 ms</strong>, same 151 tests either way.
	 *
	 * <p>It is a latency knob on the client and not a behavior SeaStar can observe, let alone one
	 * this suite asserts on, which is the same reason the container backend raises the request
	 * timeout. Everything else is stock, and {@code DriverSessionTest#testStockConfiguration} is
	 * the test that keeps a genuinely unconfigured driver covered.
	 *
	 * @param server  the listener to connect to
	 * @param version the protocol version to pin, or {@code null} to let the driver negotiate
	 * @return a session pointed at it
	 */
	static CqlSession connect(final SeaStarProtocolServer server,
		final @Nullable String version) {
		// The init and request budgets are raised for the same reason the container backend raises
		// its request timeout: classes run concurrently, and a handshake that loses the CPU for
		// half a second is a dead pool, not a fidelity finding.
		final var config = DriverConfigLoader.programmaticBuilder()
			.withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofMillis(1))
			.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(30))
			.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30));
		if (version != null) {
			config.withString(DefaultDriverOption.PROTOCOL_VERSION, version);
		}

		return CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()))
			.withLocalDatacenter("datacenter1")
			.withConfigLoader(config.build())
			.build();
	}

}
