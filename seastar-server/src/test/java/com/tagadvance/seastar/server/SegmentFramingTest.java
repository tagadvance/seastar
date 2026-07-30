package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.Segment;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * f_plan F1, at the level the driver cannot show: the bytes. A v5 connection starts in the legacy
 * framing the handshake uses at every version, and switches to CRC-checked segments for everything
 * after {@code READY} - mid-stream, on the same socket.
 *
 * <p>{@link WireClient} drives {@code SegmentCodec} by hand for this, which is what lets a test send
 * a segment whose checksum is wrong on purpose. Nothing else can produce one: the loopback socket
 * does not corrupt anything, which is the argument for checking rather than the argument against.
 *
 * <p>It is also what lets a test reach the <em>other</em> shape a segment takes. A frame longer than
 * {@link Segment#MAX_PAYLOAD_LENGTH} travels as a run of slices that only mean anything once they are
 * all in, and the two directions get there for different reasons: a server answers with more than
 * 128 KiB long before a client asks with it, because paging is deliberately not implemented and a
 * node may legally return everything. Both are exercised below, and both assert on the reassembled
 * content rather than on the absence of an exception - a silently truncated or reordered payload is
 * the failure worth catching.
 */
class SegmentFramingTest {

	private static final int V4 = ProtocolConstants.Version.V4;
	private static final int V5 = ProtocolConstants.Version.V5;

	/**
	 * Enough rows of {@link #value(int)} to put the answer well past {@link Segment#MAX_PAYLOAD_LENGTH}
	 * - four slices' worth rather than two, so that reassembly is exercised in the middle of a run and
	 * not only at its ends.
	 */
	private static final int ROWS = 1_200;

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
	@DisplayName("the handshake is legacy-framed at v5, and everything after READY is a segment")
	void testTheSwitchHappensAfterReady() throws IOException {
		try (final var client = new WireClient(server.port())) {
			// Unsegmented, on both sides: had the server switched a message early, this would hang
			// waiting for a legacy header that never comes.
			assertInstanceOf(Supported.class, client.send(V5, 1, Options.INSTANCE).message);
			assertInstanceOf(Ready.class, client.send(V5, 2, new Startup()).message);

			client.segments();
			final var response = client.send(V5, 3, new Query("SELECT cluster_name FROM system.local"));

			assertEquals(V5, response.protocolVersion);
			assertEquals(3, response.streamId);
			assertEquals(1, assertInstanceOf(Rows.class, response.message).getData().size());
		}
	}

	@Test
	@DisplayName("a v4 connection never switches, and stays legacy-framed for its whole life")
	void testV4StaysLegacy() throws IOException {
		try (final var client = new WireClient(server.port())) {
			assertInstanceOf(Ready.class, client.send(V4, 1, new Startup()).message);
			final var response = client.send(V4, 2, new Query("SELECT cluster_name FROM system.local"));

			assertEquals(V4, response.protocolVersion);
			assertInstanceOf(Rows.class, response.message);
		}
	}

	@Test
	@DisplayName("statements run over segments exactly as they do over frames")
	void testStatementsOverSegments() throws IOException {
		try (final var client = connect()) {
			assertInstanceOf(SchemaChange.class, client.send(V5, 1, new Query(
				"CREATE KEYSPACE ks WITH replication = "
					+ "{'class':'SimpleStrategy','replication_factor':1}")).message);
			assertInstanceOf(SchemaChange.class, client.send(V5, 2,
				new Query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)")).message);
			client.send(V5, 3, new Query("INSERT INTO ks.t (id, v) VALUES (1, 'over a segment')"));

			final var rows = assertInstanceOf(Rows.class,
				client.send(V5, 4, new Query("SELECT v FROM ks.t")).message);

			assertEquals(1, rows.getData().size());
		}
	}

	@Test
	@DisplayName("several requests in flight come back on their own stream ids over segments")
	void testSeveralInFlight() throws IOException {
		try (final var client = connect()) {
			for (int streamId = 1; streamId <= 5; streamId++) {
				client.write(V5, streamId, false, Options.INSTANCE);
			}

			for (int i = 0; i < 5; i++) {
				final var response = client.read();
				assertInstanceOf(Supported.class, response.message);
				assertTrue(response.streamId >= 1 && response.streamId <= 5,
					"unexpected stream id " + response.streamId);
			}
		}
	}

	@Test
	@DisplayName("a segment whose payload does not match its CRC32 is refused and the socket closed")
	void testCorruptPayload() throws IOException {
		try (final var client = connect()) {
			final var bytes = client.encodeSegment(V5, 1, Options.INSTANCE);
			// The last byte of the payload, which is in front of the four-byte trailing checksum. The
			// checksum stays as it was computed, so the two no longer agree.
			bytes[bytes.length - 5] ^= 0x01;
			client.writeRaw(bytes);

			final var error = assertInstanceOf(Error.class, client.read().message);
			assertEquals(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, error.code);
			assertTrue(error.message.contains("CRC mismatch"), error.message);
			assertTrue(error.message.contains("payload"), error.message);

			assertThrows(IOException.class, () -> client.send(V5, 2, Options.INSTANCE),
				"the byte stream is no longer trustworthy, so the connection has to end");
		}
	}

	@Test
	@DisplayName("a segment whose header does not match its CRC24 is refused before it is believed")
	void testCorruptHeader() throws IOException {
		try (final var client = connect()) {
			final var bytes = client.encodeSegment(V5, 1, Options.INSTANCE);
			// Byte 0 is part of the declared payload length. Trusting it would have the decoder wait
			// for a length that never arrives, which is why the CRC is checked before the length is
			// used rather than after the payload has been read.
			bytes[0] ^= 0x20;
			client.writeRaw(bytes);

			final var error = assertInstanceOf(Error.class, client.read().message);
			assertEquals(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, error.code);
			assertTrue(error.message.contains("CRC mismatch"), error.message);
			assertTrue(error.message.contains("header"), error.message);
		}
	}

	@Test
	@DisplayName("an intact segment is still served after the CRC check has looked at it")
	void testTheCheckDoesNotEatGoodSegments() throws IOException {
		try (final var client = connect()) {
			client.writeRaw(client.encodeSegment(V5, 9, Options.INSTANCE));
			final var response = client.read();

			assertEquals(9, response.streamId);
			assertInstanceOf(Supported.class, response.message);
		}
	}

	@Test
	@DisplayName("a result set too large for one segment comes back whole, across several slices")
	void testAResponseSplitAcrossSegments() throws IOException {
		try (final var client = connect()) {
			client.send(V5, 1, new Query("CREATE KEYSPACE ks WITH replication = "
				+ "{'class':'SimpleStrategy','replication_factor':1}"));
			client.send(V5, 2, new Query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"));
			// Seeded in process so that this test says something about one direction only. Every row is
			// far short of a segment on its own; it is the answer as a whole that does not fit, which is
			// the case a server reaches long before a client does.
			final var insert = session.prepare("INSERT INTO ks.t (id, v) VALUES (?, ?)");
			for (int id = 0; id < ROWS; id++) {
				session.execute(insert.bind(id, value(id)));
			}

			final var rows = assertInstanceOf(Rows.class,
				client.send(V5, 3, new Query("SELECT id, v FROM ks.t")).message);

			assertEquals(ROWS, rows.getData().size());
			assertTrue(client.slicesRead() >= 4, "expected the answer to be split across several "
				+ "segments, got " + client.slicesRead() + " slices in " + client.segmentsRead()
				+ " segments");
			// Every value is a function of its own row's id, so a slice dropped, duplicated or put back
			// in the wrong order shows up as a value that does not belong to the row it arrived in -
			// which is what a length check alone would miss.
			final var seen = new HashSet<Integer>();
			for (final var row : rows.getData()) {
				final var id = row.get(0).duplicate().getInt();
				assertEquals(value(id), text(row.get(1)), "row " + id + " came back mangled");
				assertTrue(seen.add(id), "row " + id + " came back twice");
			}
			assertEquals(ROWS, seen.size());

			// The client's accumulation has to reset, and so does the listener's encoder.
			assertInstanceOf(Supported.class, client.send(V5, 4, Options.INSTANCE).message);
		}
	}

	@Test
	@DisplayName("a statement too large for one segment is reassembled before it is run")
	void testARequestSplitAcrossSegments() throws IOException {
		// Three slices' worth. A text literal rather than a bound value because the frame is the frame:
		// what is under test is the far side putting the slices back in order, not how they were filled.
		final var value = "the quick brown fox-".repeat(15_000);
		try (final var client = connect()) {
			client.send(V5, 1, new Query("CREATE KEYSPACE ks WITH replication = "
				+ "{'class':'SimpleStrategy','replication_factor':1}"));
			client.send(V5, 2, new Query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"));

			assertInstanceOf(Void.class, client.send(V5, 3,
				new Query("INSERT INTO ks.t (id, v) VALUES (1, '" + value + "')")).message);

			assertTrue(client.slicesWritten() >= 3, "expected the request to be split across several "
				+ "segments, got " + client.slicesWritten() + " slices");
			// Read back in process, so that a mangled answer cannot be mistaken for a mangled request. A
			// statement whose slices arrived out of order would not have parsed at all; one whose last
			// slice was lost would never have run; a truncated value would have inserted quietly.
			final var row = session.execute("SELECT v FROM ks.t WHERE id = 1").one();
			assertNotNull(row);
			assertEquals(value, row.getString("v"));

			// The listener's decoder has to have reset its accumulation, or this hangs.
			assertEquals(1, assertInstanceOf(Rows.class,
				client.send(V5, 4, new Query("SELECT id FROM ks.t")).message).getData().size());
		}
	}

	@Test
	@DisplayName("PREPARE carries a result metadata id at v5, and none at v4")
	void testResultMetadataId() throws IOException {
		try (final var client = connect()) {
			client.send(V5, 1, new Query("CREATE KEYSPACE ks WITH replication = "
				+ "{'class':'SimpleStrategy','replication_factor':1}"));
			client.send(V5, 2, new Query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"));

			final var prepared = assertInstanceOf(Prepared.class,
				client.send(V5, 3, new Prepare("SELECT * FROM ks.t WHERE id = ?")).message);
			assertNotNull(prepared.resultMetadataId);
			assertTrue(prepared.resultMetadataId.length > 0);

			// Twice, because the id is a buffer the core hands out by reference: without the duplicate
			// on the way onto the wire the second answer would carry an empty one.
			final var again = assertInstanceOf(Prepared.class,
				client.send(V5, 4, new Prepare("SELECT * FROM ks.t WHERE id = ?")).message);
			assertEquals(List.of(prepared.resultMetadataId.length),
				List.of(again.resultMetadataId.length));
		}
	}

	@Test
	@DisplayName("a v4 PREPARE of the same statement still carries no result metadata id")
	void testNoResultMetadataIdAtV4() throws IOException {
		try (final var client = new WireClient(server.port())) {
			client.send(V4, 1, new Startup());
			client.send(V4, 2, new Query("CREATE KEYSPACE ks WITH replication = "
				+ "{'class':'SimpleStrategy','replication_factor':1}"));
			client.send(V4, 3, new Query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)"));

			final var prepared = assertInstanceOf(Prepared.class,
				client.send(V4, 4, new Prepare("SELECT * FROM ks.t WHERE id = ?")).message);

			assertNull(prepared.resultMetadataId);
		}
	}

	@Test
	@DisplayName("duration is described by its own protocol code at v5, not as a marshaller name")
	void testDurationIsAPrimitiveAtV5() throws IOException {
		try (final var client = connect()) {
			client.send(V5, 1, new Query("CREATE KEYSPACE ks WITH replication = "
				+ "{'class':'SimpleStrategy','replication_factor':1}"));
			client.send(V5, 2, new Query("CREATE TABLE ks.t (id int PRIMARY KEY, d duration)"));

			final var rows = assertInstanceOf(Rows.class,
				client.send(V5, 3, new Query("SELECT d FROM ks.t")).message);

			assertEquals(RawType.PRIMITIVES.get(ProtocolConstants.DataType.DURATION),
				rows.getMetadata().columnSpecs.get(0).type);
		}
	}

	/**
	 * @param id the row the value belongs to
	 * @return half a kilobyte that says which row it came from, so that a value arriving in the wrong
	 *     row is a failure rather than a coincidence
	 */
	private static String value(final int id) {
		return ("row-" + id + "-").repeat(48);
	}

	private static String text(final ByteBuffer value) {
		return StandardCharsets.UTF_8.decode(value.duplicate()).toString();
	}

	/**
	 * @return a client that has completed a v5 handshake and switched to segments, exactly as the
	 *     listener has on the other side
	 */
	private WireClient connect() throws IOException {
		final var client = new WireClient(server.port());
		assertInstanceOf(Ready.class, client.send(V5, 0, new Startup()).message);
		client.segments();

		return client;
	}
}
