package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * f_plan F2: a DDL statement is answered with a {@code SCHEMA_CHANGE} result on the connection that
 * ran it <em>and</em> pushed as a {@code SCHEMA_CHANGE} event to every connection that registered
 * for one. Both, not either - the result is how the client that changed the schema finds out, the
 * event is how everybody else does.
 *
 * <p>Two connections throughout, because one connection cannot tell the two apart.
 *
 * <p>What a {@code REGISTER} accepts and how it refuses came off a {@code cassandra:5.0.8} container
 * over a raw socket - the error's exact wording, that the name is resolved case-insensitively, and
 * that one bad name rejects the whole message. No driver sends a bad type, so nothing here would have
 * been caught by using one.
 */
class SchemaEventTest {

	private static final int V4 = ProtocolConstants.Version.V4;

	private static final String CREATE_KEYSPACE = "CREATE KEYSPACE ks WITH replication = "
		+ "{'class':'SimpleStrategy','replication_factor':1}";

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
	@DisplayName("a watching connection is told about DDL run on a different connection")
	void testEventReachesASecondConnection() throws IOException {
		try (final var watcher = watcher(); final var worker = connect()) {
			final var result = worker.send(V4, 1, new Query(CREATE_KEYSPACE));

			// Both halves of the requirement: the result on the connection that ran it ...
			final var change = assertInstanceOf(SchemaChange.class, result.message);
			assertEquals(ProtocolConstants.SchemaChangeType.CREATED, change.changeType);
			assertEquals("ks", change.keyspace);

			// ... and the event on the connection that asked to be told.
			final var event = assertInstanceOf(SchemaChangeEvent.class, watcher.read().message);
			assertEquals(ProtocolConstants.SchemaChangeType.CREATED, event.changeType);
			assertEquals(ProtocolConstants.SchemaChangeTarget.KEYSPACE, event.target);
			assertEquals("ks", event.keyspace);
			assertNull(event.object);
		}
	}

	@Test
	@DisplayName("an event travels on a negative stream id, so no caller mistakes it for an answer")
	void testEventStreamId() throws IOException {
		try (final var watcher = watcher(); final var worker = connect()) {
			worker.send(V4, 7, new Query(CREATE_KEYSPACE));

			assertTrue(watcher.read().streamId < 0);
		}
	}

	@Test
	@DisplayName("every kind of schema change is published, naming the object that moved")
	void testEveryKindOfChange() throws IOException {
		try (final var watcher = watcher(); final var worker = connect()) {
			worker.send(V4, 1, new Query(CREATE_KEYSPACE));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.CREATED,
				ProtocolConstants.SchemaChangeTarget.KEYSPACE, null);

			worker.send(V4, 2, new Query("CREATE TYPE ks.address (street text, city text)"));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.CREATED,
				ProtocolConstants.SchemaChangeTarget.TYPE, "address");

			worker.send(V4, 3, new Query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.CREATED,
				ProtocolConstants.SchemaChangeTarget.TABLE, "t");

			worker.send(V4, 4, new Query("ALTER TABLE ks.t ADD w text"));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.UPDATED,
				ProtocolConstants.SchemaChangeTarget.TABLE, "t");

			// An index is not a schema-change target of its own: what a driver has to refresh is the
			// table it is on, which is what the result says too.
			worker.send(V4, 5, new Query("CREATE INDEX t_v_idx ON ks.t (v)"));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.UPDATED,
				ProtocolConstants.SchemaChangeTarget.TABLE, "t");

			worker.send(V4, 6, new Query("DROP TABLE ks.t"));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.DROPPED,
				ProtocolConstants.SchemaChangeTarget.TABLE, "t");

			worker.send(V4, 7, new Query("DROP KEYSPACE ks"));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.DROPPED,
				ProtocolConstants.SchemaChangeTarget.KEYSPACE, null);
		}
	}

	@Test
	@DisplayName("a statement that changes nothing publishes nothing")
	void testNoEventWithoutAChange() throws IOException {
		try (final var watcher = watcher(); final var worker = connect()) {
			worker.send(V4, 1, new Query(CREATE_KEYSPACE));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.CREATED,
				ProtocolConstants.SchemaChangeTarget.KEYSPACE, null);
			worker.send(V4, 2, new Query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"));
			assertEvent(watcher, ProtocolConstants.SchemaChangeType.CREATED,
				ProtocolConstants.SchemaChangeTarget.TABLE, "t");

			// DROP INDEX IF EXISTS on an index that is not there is the one DDL statement SeaStar
			// knows changed nothing, and a node answers it VOID rather than with a schema change.
			worker.send(V4, 3, new Query("DROP INDEX IF EXISTS ks.absent_idx"));

			assertNothingPending(watcher);
		}
	}

	@Test
	@DisplayName("a connection that never registered is not written to")
	void testUnregisteredConnectionIsLeftAlone() throws IOException {
		try (final var quiet = connect(); final var worker = connect()) {
			worker.send(V4, 1, new Query(CREATE_KEYSPACE));

			assertNothingPending(quiet);
		}
	}

	@Test
	@DisplayName("registering for topology or status alone gets no schema events, and never fires")
	void testOtherEventTypesNeverFire() throws IOException {
		try (final var quiet = connect(); final var worker = connect()) {
			quiet.send(V4, 2, new Register(List.of(ProtocolConstants.EventType.TOPOLOGY_CHANGE,
				ProtocolConstants.EventType.STATUS_CHANGE)));

			worker.send(V4, 1, new Query(CREATE_KEYSPACE));

			assertNothingPending(quiet);
		}
	}

	@Test
	@DisplayName("the connection that ran the DDL is told too, if it registered")
	void testTheOriginatingConnectionIsToldAsWell() throws IOException {
		try (final var watcher = watcher()) {
			watcher.write(V4, 3, false, new Query(CREATE_KEYSPACE));

			// Read as a stream-id-to-message mapping rather than as a sequence. An event may be sent
			// at any time, including between a request and its response, so which of the two arrives
			// first is not something the protocol lets a client depend on.
			final var answers = new HashMap<Integer, Message>();
			answers.putAll(read(watcher));
			answers.putAll(read(watcher));

			assertInstanceOf(SchemaChange.class, answers.get(3));
			assertInstanceOf(SchemaChangeEvent.class, answers.get(-1));
		}
	}

	private static Map<Integer, Message> read(final WireClient client) throws IOException {
		final var frame = client.read();

		return Map.of(frame.streamId, frame.message);
	}

	@Test
	@DisplayName("a second REGISTER adds to the first rather than replacing it")
	void testRegisterIsCumulative() throws IOException {
		try (final var watcher = connect(); final var worker = connect()) {
			watcher.send(V4, 2, new Register(List.of(ProtocolConstants.EventType.TOPOLOGY_CHANGE)));
			watcher.send(V4, 3, new Register(List.of(ProtocolConstants.EventType.SCHEMA_CHANGE)));

			worker.send(V4, 1, new Query(CREATE_KEYSPACE));

			assertInstanceOf(SchemaChangeEvent.class, watcher.read().message);
		}
	}

	@Test
	@DisplayName("REGISTER naming an event type that does not exist is a PROTOCOL_ERROR")
	void testRegisterWithAnUnknownEventType() throws IOException {
		try (final var client = connect()) {
			final var response = client.send(V4, 2, new Register(List.of("MOON_PHASE_CHANGE")));
			final var error = assertInstanceOf(Error.class, response.message);

			assertEquals(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, error.code);
			// The whole message, not a substring: this is what cassandra:5.0.8 answers, verbatim.
			assertEquals("Invalid value 'MOON_PHASE_CHANGE' for Type", error.message);
		}
	}

	@Test
	@DisplayName("the refusal quotes the event type as it was sent, whatever case that was")
	void testTheRefusalQuotesWhatWasSent() throws IOException {
		try (final var client = connect()) {
			final var error = assertInstanceOf(Error.class,
				client.send(V4, 2, new Register(List.of("moon_phase_change"))).message);

			assertEquals("Invalid value 'moon_phase_change' for Type", error.message);
		}
	}

	@Test
	@DisplayName("an event type is resolved case-insensitively, and is then honoured")
	void testTheEventTypeIsCaseInsensitive() throws IOException {
		try (final var watcher = connect(); final var other = connect()) {
			// A node upper-cases the name before looking it up, so this registers rather than being
			// refused. Captured from cassandra:5.0.8, which then pushes the event to it.
			assertInstanceOf(Ready.class,
				watcher.send(V4, 2, new Register(List.of("schema_change"))).message);

			other.send(V4, 2, new Query(CREATE_KEYSPACE));

			assertEvent(watcher, ProtocolConstants.SchemaChangeType.CREATED,
				ProtocolConstants.SchemaChangeTarget.KEYSPACE, null);
		}
	}

	@Test
	@DisplayName("one unknown event type rejects the whole REGISTER, including the names beside it")
	void testOneBadNameRejectsTheRest() throws IOException {
		try (final var watcher = connect(); final var other = connect()) {
			assertInstanceOf(Error.class, watcher.send(V4, 2, new Register(
				List.of(ProtocolConstants.EventType.SCHEMA_CHANGE, "MOON_PHASE_CHANGE"))).message);

			other.send(V4, 2, new Query(CREATE_KEYSPACE));

			assertNothingPending(watcher);
		}
	}

	private WireClient connect() throws IOException {
		final var client = new WireClient(server.port());
		client.send(V4, 1, new Startup());

		return client;
	}

	private WireClient watcher() throws IOException {
		final var client = connect();
		assertInstanceOf(Ready.class,
			client.send(V4, 2, new Register(List.of(ProtocolConstants.EventType.SCHEMA_CHANGE))).message);

		return client;
	}

	private static void assertEvent(final WireClient client, final String changeType,
		final String target, final String object) throws IOException {
		final var event = assertInstanceOf(SchemaChangeEvent.class, client.read().message);

		assertEquals(changeType, event.changeType);
		assertEquals(target, event.target);
		assertEquals("ks", event.keyspace);
		assertEquals(object, event.object);
	}

	/**
	 * Asserts that nothing is waiting to be read on this connection, by asking a question whose
	 * answer is unmistakable: an event would be sitting in front of the {@code SUPPORTED} and would
	 * be what came back instead.
	 */
	private static void assertNothingPending(final WireClient client) throws IOException {
		assertInstanceOf(Supported.class, client.send(V4, 99, Options.INSTANCE).message);
	}
}
