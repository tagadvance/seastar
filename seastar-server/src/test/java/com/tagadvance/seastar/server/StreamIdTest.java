package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The stream id is what lets a client have several requests on one connection at once,
 * and it is the only thing that says which answer belongs to which.
 *
 * <p>A driver manages stream ids itself and will not put an arbitrary mixture in flight on demand,
 * so this speaks the protocol directly. It asserts on the id-to-answer mapping rather than on the
 * order the answers arrive in: the funnel happens to complete them in arrival order today, and a
 * test that pinned that would be pinning an implementation detail the protocol does not require.
 */
class StreamIdTest {

	private static final int V4 = ProtocolConstants.Version.V4;

	private SeaStarCqlSession session;

	private SeaStarProtocolServer server;

	private WireClient client;

	@BeforeEach
	void setUp() throws IOException {
		session = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(session).build().start();
		client = new WireClient(server.port());
		client.send(V4, 0, new Startup());
	}

	@AfterEach
	void tearDown() throws IOException {
		client.close();
		server.close();
		session.close();
	}

	@Test
	@DisplayName("requests put in flight together each get their own stream id back")
	void testEveryAnswerCarriesItsOwnStreamId() throws IOException {
		client.send(V4, 1, new Query("CREATE KEYSPACE ks WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}"));
		client.send(V4, 2, new Query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"));

		// Deliberately mixed: the answers are of different kinds and different sizes, so a response
		// that came back under the wrong id would be visible as the wrong kind rather than as a value
		// that happens to look the same. The DDL is the slow one, and it is sent first.
		final var expected = new LinkedHashMap<Integer, Class<? extends Message>>();
		expected.put(11, SchemaChange.class);
		expected.put(12, Void.class);
		expected.put(13, Rows.class);
		expected.put(14, Error.class);
		expected.put(15, Prepared.class);
		expected.put(16, SetKeyspace.class);
		expected.put(17, Rows.class);

		client.write(V4, 11, false, new Query("CREATE TABLE ks.u (id int PRIMARY KEY)"));
		client.write(V4, 12, false, new Query("INSERT INTO ks.t (id, v) VALUES (1, 'one')"));
		client.write(V4, 13, false, new Query("SELECT * FROM ks.t"));
		client.write(V4, 14, false, new Query("SELECT * FROM ks.nope"));
		client.write(V4, 15, false, new Prepare("SELECT * FROM ks.t WHERE id = ?"));
		client.write(V4, 16, false, new Query("USE ks"));
		client.write(V4, 17, false, new Query("SELECT * FROM ks.u"));

		final var answers = new HashMap<Integer, Message>();
		for (int i = 0; i < expected.size(); i++) {
			final var frame = client.read();
			assertEquals(V4, frame.protocolVersion);
			answers.put(frame.streamId, frame.message);
		}

		assertEquals(expected.keySet(), answers.keySet(),
			"every request's id should come back exactly once");
		for (final var entry : expected.entrySet()) {
			assertInstanceOf(entry.getValue(), answers.get(entry.getKey()),
				"stream " + entry.getKey() + " got another request's answer");
		}
	}

	@Test
	@DisplayName("the same stream id is reusable once its answer has come back")
	void testStreamIdIsReusable() throws IOException {
		// A client with a small pool recycles ids constantly, so an id remembered from a previous
		// request would break the second use rather than the first.
		final var keyspace = client.send(V4, 5, new Query("CREATE KEYSPACE reuse WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}"));
		assertEquals(5, keyspace.streamId);
		assertInstanceOf(SchemaChange.class, keyspace.message);

		final var table = client.send(V4, 5,
			new Query("CREATE TABLE reuse.t (id int PRIMARY KEY)"));
		assertEquals(5, table.streamId);
		assertInstanceOf(SchemaChange.class, table.message);

		final var select = client.send(V4, 5, new Query("SELECT * FROM reuse.t"));
		assertEquals(5, select.streamId);
		assertInstanceOf(Rows.class, select.message);
	}

}
