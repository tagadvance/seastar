package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.tagadvance.seastar.AbstractCqlSessionTest;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The third backend for the fidelity suite: the same expectations {@code SeaStarCqlSessionTest} runs
 * in process and {@code ContainerCqlSessionTest} runs against a real node, run again over a socket.
 * In-process SeaStar and wire SeaStar have to be indistinguishable through the driver API, and every
 * statement type, error path and type mapping the suite already covers becomes a protocol
 * conformance test for free.
 *
 * <p>The session is otherwise stock - contact point and datacenter, schema metadata left on - so the
 * suite also exercises the driver's own request pipeline, metadata refresh and paging termination.
 * The one thing configured is the schema debounce window; see {@link #connect(SeaStarProtocolServer)}
 * for why, and for what it costs to leave alone.
 *
 * <p>No Docker, so this runs on the default {@code test} task rather than behind the
 * {@code container} tag.
 */
class WireCqlSessionTest extends AbstractCqlSessionTest {

	private SeaStarCqlSession backing;

	private SeaStarProtocolServer server;

	/**
	 * An extension rather than an {@code @AfterAll} method: extension callbacks run after every
	 * {@code @AfterAll}, so the driver session the suite owns is closed while the server is still
	 * listening.
	 */
	@RegisterExtension
	final AfterAllCallback shutdown = context -> {
		server.close();
		backing.close();
	};

	@Override
	protected CqlSession createInstance() {
		backing = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(backing).build().start();

		return connect(server);
	}

	/**
	 * The suite is run over a session that negotiated its own version and settled on v5, so
	 * {@code hasResultMetadataId()} is left at its default and
	 * {@code testResultMetadataIdIsReadable} runs rather than skipping.
	 *
	 * <p>This is the other half of it, and the reason the skip was never the whole story: v4 is
	 * still served, a driver pinned to it still gets null, and that is the driver's own documented
	 * contract rather than a gap. Stands up its own server rather than borrowing the suite's, so
	 * that it does not depend on where in the ordered suite it lands.
	 */
	@Test
	@DisplayName("a session pinned to v4 gets no result metadata id, as the driver documents")
	void testNoResultMetadataIdAtV4() {
		try (final var isolated = SeaStarCqlSession.builder().build();
			final var listener = SeaStarProtocolServer.builder().session(isolated).build().start();
			final var connected = connect(listener, "V4")) {
			assertEquals("V4", connected.getContext().getProtocolVersion().name());
			connected.execute("CREATE KEYSPACE v4 WITH replication = "
				+ "{'class':'SimpleStrategy','replication_factor':1}");
			connected.execute("CREATE TABLE v4.t (id int PRIMARY KEY, name text)");

			assertNull(connected.prepare("SELECT * FROM v4.t WHERE id = ?").getResultMetadataId());
		}
	}

	@Test
	@DisplayName("an unpinned session against this listener negotiates protocol v5")
	void testTheSuiteRunsOverV5() {
		try (final var isolated = SeaStarCqlSession.builder().build();
			final var listener = SeaStarProtocolServer.builder().session(isolated).build().start();
			final var connected = connect(listener)) {
			assertEquals("V5", connected.getContext().getProtocolVersion().name());
		}
	}

	/**
	 * The driver debounces its schema refresh by a second, and it holds a DDL statement's answer
	 * until the refresh it triggered has completed - so the suite, which is mostly DDL, spent a
	 * second per statement waiting for a window rather than for SeaStar. Measured: <strong>190 s
	 * with the default window, 6.9 s at 1 ms</strong>, same 151 tests either way.
	 *
	 * <p>It is a latency knob on the client and not a behavior SeaStar can observe, let alone one
	 * this suite asserts on, which is the same reason {@code ContainerCqlSessionTest} raises the
	 * request timeout. Everything else is stock, and {@code DriverSessionTest#testStockConfiguration}
	 * is the test that keeps a genuinely unconfigured driver covered.
	 *
	 * @param server the listener to connect to
	 * @return a session pointed at it
	 */
	private static CqlSession connect(final SeaStarProtocolServer server) {
		return connect(server, null);
	}

	/**
	 * @param server  the listener to connect to
	 * @param version the protocol version to pin, or {@code null} to let the driver negotiate
	 * @return a session pointed at it
	 */
	private static CqlSession connect(final SeaStarProtocolServer server,
		final @Nullable String version) {
		final var config = DriverConfigLoader.programmaticBuilder()
			.withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofMillis(1));
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
