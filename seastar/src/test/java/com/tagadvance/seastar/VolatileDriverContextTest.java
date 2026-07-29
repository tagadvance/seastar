package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The protocol version an in-process session reports, and why it is a decision rather than a
 * placeholder. See {@link VolatileDriverContext#getProtocolVersion()} for the reasoning; this pins
 * the two claims it rests on so that a later bump has to argue with a test.
 */
class VolatileDriverContextTest {

	private SeaStarCqlSession session;

	@BeforeEach
	void setUp() {
		session = SeaStarCqlSession.builder().build();
		session.execute("CREATE KEYSPACE ctx WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}");
		session.execute("CREATE TABLE ctx.t (id int PRIMARY KEY, v text, tags list<int>, "
			+ "d duration)");
	}

	@AfterEach
	void tearDown() {
		session.close();
	}

	@Test
	@DisplayName("an in-process session encodes at the driver's default version, not at a wire one")
	void testProtocolVersion() {
		assertEquals(ProtocolVersion.DEFAULT, session.getContext().getProtocolVersion(),
			"this is the codec version of a session that is on no protocol at all. seastar-server "
				+ "serves v4 and v5 and one session can back both at once, so a wire version cannot "
				+ "be reported here; it belongs to the connection.");
	}

	@Test
	@DisplayName("a stored value decodes to the same thing at v4 and at v5")
	void testEncodingIsTheSameAtBothServedVersions() {
		// The claim that makes the version above harmless: seastar-server hands a client the bytes
		// the core stored, without re-encoding them, whichever version that client is on.
		session.execute("INSERT INTO ctx.t (id, v, tags, d) "
			+ "VALUES (1, 'a', [1, 2, 3], 89h4m48s)");
		final var row = session.execute("SELECT * FROM ctx.t").one();
		assertNotNull(row);

		final var definitions = row.getColumnDefinitions();
		for (int i = 0; i < definitions.size(); i++) {
			final var codec = CodecRegistry.DEFAULT.codecFor(definitions.get(i).getType());
			final var bytes = row.getBytesUnsafe(i);
			assertNotNull(bytes, definitions.get(i).getName().asInternal());

			assertEquals(codec.decode(bytes.duplicate(), DefaultProtocolVersion.V4),
				codec.decode(bytes.duplicate(), DefaultProtocolVersion.V5),
				definitions.get(i).getName().asInternal());
		}
	}
}
