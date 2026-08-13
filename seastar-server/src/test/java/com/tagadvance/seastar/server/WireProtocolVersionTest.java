package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.tagadvance.seastar.SeaStarCqlSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The protocol-version half of what {@code WireHarness} leaves at its defaults: the fidelity suite
 * negotiates its way to v5, and a driver pinned to v4 is still served - with the result metadata id
 * absent, which is the driver's own documented contract rather than a gap.
 */
class WireProtocolVersionTest {

	@Test
	@DisplayName("a session pinned to v4 gets no result metadata id, as the driver documents")
	void testNoResultMetadataIdAtV4() {
		try (final var isolated = SeaStarCqlSession.builder().build();
			final var listener = SeaStarProtocolServer.builder().session(isolated).build().start();
			final var connected = WireHarness.connect(listener, "V4")) {
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
			final var connected = WireHarness.connect(listener, null)) {
			assertEquals("V5", connected.getContext().getProtocolVersion().name());
		}
	}

}
