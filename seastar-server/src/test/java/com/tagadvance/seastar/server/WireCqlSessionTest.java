package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.tagadvance.seastar.AbstractCqlSessionTest;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
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

	@Override
	protected boolean hasResultMetadataId() {
		return false;
	}

	/**
	 * The other half of {@link #hasResultMetadataId()}: the suite skips its assertion, so this pins
	 * what happens instead, and it is written to fail rather than to pass silently once the listener
	 * speaks v5. At that point the identifier starts arriving, this is the test that says so, and the
	 * override above comes out.
	 *
	 * <p>Stands up its own server rather than borrowing the suite's, so that it does not depend on
	 * where in the ordered suite it lands.
	 */
	@Test
	@DisplayName("no result metadata id arrives, because the listener speaks protocol v4")
	void testNoResultMetadataIdAtV4() {
		try (final var isolated = SeaStarCqlSession.builder().build();
			final var listener = SeaStarProtocolServer.builder().session(isolated).build().start();
			final var connected = connect(listener)) {
			connected.execute("CREATE KEYSPACE v4 WITH replication = "
				+ "{'class':'SimpleStrategy','replication_factor':1}");
			connected.execute("CREATE TABLE v4.t (id int PRIMARY KEY, name text)");

			assertNull(connected.prepare("SELECT * FROM v4.t WHERE id = ?").getResultMetadataId(),
				"a v5 listener would answer this, and the suite's assertion should be re-enabled");
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
		return CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()))
			.withLocalDatacenter("datacenter1")
			.withConfigLoader(DriverConfigLoader.programmaticBuilder()
				.withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofMillis(1))
				.build())
			.build();
	}

}
