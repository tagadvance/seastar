package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.tagadvance.seastar.AbstractCqlSessionTest;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
 * <p>The session is stock - contact point and datacenter only, no config loader - so the suite also
 * exercises the driver's own request pipeline, schema metadata refresh and paging termination.
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

	private static CqlSession connect(final SeaStarProtocolServer server) {
		return CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()))
			.withLocalDatacenter("datacenter1")
			.build();
	}

}
