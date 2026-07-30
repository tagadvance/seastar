package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The whole of the wire path, exercised the way a harness will: an ordinary {@link CqlSession}
 * seeding a schema, writing rows and reading them back over a socket.
 *
 * <p>This is the first thing in the repository to run the driver's <em>request pipeline</em> rather
 * than only its codecs. Everything up to here went through {@code WireClient}, which decodes a
 * response and hands over the message; a real session builds {@code ColumnDefinitions} out of the
 * rows metadata, wraps the page in a {@code DefaultAsyncResultSet}, and decides for itself whether
 * there is another page to ask for. None of that was covered before.
 */
@NullMarked
class DriverSessionTest {

	private static final String KEYSPACE = "CREATE KEYSPACE harness WITH replication = "
		+ "{'class':'SimpleStrategy','replication_factor':1}";
	private static final String TABLE =
		"CREATE TABLE harness.t (id int, name text, tags set<text>, PRIMARY KEY (id))";

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
	@DisplayName("a session seeds a schema, writes rows and reads them back over the wire")
	void testRoundTrip() {
		try (final var connected = connect(false)) {
			connected.execute(KEYSPACE);
			connected.execute(TABLE);
			connected.execute("INSERT INTO harness.t (id, name, tags) VALUES (1, 'one', {'a','b'})");
			connected.execute(
				connected.prepare("INSERT INTO harness.t (id, name) VALUES (?, ?)").bind(2, "two"));

			final var rows = connected.execute("SELECT id, name, tags FROM harness.t").all();

			assertEquals(2, rows.size());
			assertEquals(List.of(1, 2), rows.stream().map(row -> row.getInt("id")).toList());
			assertEquals(List.of("one", "two"), rows.stream().map(row -> row.getString("name"))
				.toList());
			assertEquals(Set.of("a", "b"), rows.get(0).getSet("tags", String.class));
			// A column with no value comes back null rather than absent, which is the whole of what
			// a set<text> that was never written looks like on a node.
			assertEquals(Set.of(), rows.get(1).getSet("tags", String.class));
		}
	}

	@Test
	@DisplayName("the in-process session sees what the wire wrote, and the wire sees its writes")
	void testTheSameSessionIsBehindBoth() {
		try (final var connected = connect(false)) {
			connected.execute(KEYSPACE);
			connected.execute(TABLE);
			connected.execute("INSERT INTO harness.t (id, name) VALUES (7, 'seven')");

			assertEquals("seven",
				session.execute("SELECT name FROM harness.t WHERE id = 7").one().getString("name"));

			session.execute("INSERT INTO harness.t (id, name) VALUES (8, 'eight')");

			assertEquals("eight", connected.execute("SELECT name FROM harness.t WHERE id = 8")
				.one()
				.getString("name"));
		}
	}

	@Test
	@DisplayName("a prepared statement reflects a column added after it was prepared")
	void testPreparedStatementAfterASchemaChange() {
		// e_plan E4, answered end to end rather than at the socket: a bound statement asks for its
		// metadata to be skipped, so if the server ever honoured that the driver would decode this row
		// against the columns the PREPARE described and never see `extra` at all.
		try (final var connected = connect(false)) {
			connected.execute(KEYSPACE);
			connected.execute(TABLE);
			connected.execute("INSERT INTO harness.t (id, name) VALUES (1, 'one')");
			final var prepared = connected.prepare("SELECT * FROM harness.t WHERE id = ?");

			connected.execute("ALTER TABLE harness.t ADD extra text");
			connected.execute("UPDATE harness.t SET extra = 'added' WHERE id = 1");
			final var row = connected.execute(prepared.bind(1)).one();

			assertNotNull(row);
			assertEquals("added", row.getString("extra"));
		}
	}

	@Test
	@DisplayName("DDL returns promptly rather than waiting out the schema-agreement timeout")
	void testDdlIsNotSlow() {
		// d_plan D5's failure mode, and it is loud only if something asserts on it: with a null or
		// unstable schema_version the driver never sees agreement, waits the full
		// advanced.control-connection.schema-agreement.timeout - ten seconds by default - and then
		// carries on successfully anyway, warning where nobody is looking. Every statement below is
		// answered from memory, so a second apiece is already two orders of magnitude of headroom.
		try (final var connected = connect(false)) {
			assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
				connected.execute(KEYSPACE);
				connected.execute(TABLE);
				connected.execute("ALTER TABLE harness.t ADD extra text");
				connected.execute("CREATE INDEX t_name_idx ON harness.t (name)");
				connected.execute("DROP TABLE harness.t");
				connected.execute("DROP KEYSPACE harness");
			});

			assertTrue(connected.checkSchemaAgreement());
		}
	}

	@Test
	@DisplayName("a session with schema metadata left on builds its own Metadata from system_schema")
	void testSchemaMetadataEnabled() {
		try (final var connected = connect(true)) {
			connected.execute(KEYSPACE);
			connected.execute(TABLE);

			final var keyspace = connected.getMetadata()
				.getKeyspace(CqlIdentifier.fromInternal("harness"))
				.orElseThrow(() -> new AssertionError("the keyspace comes from system_schema"));
			final var table = keyspace.getTable(CqlIdentifier.fromInternal("t"))
				.orElseThrow(() -> new AssertionError("the table comes from system_schema"));

			assertEquals(List.of("id", "name", "tags"),
				table.getColumns().values().stream().map(column -> column.getName().asInternal())
					.toList());
			assertEquals("id", table.getPartitionKey().get(0).getName().asInternal());
			assertNotNull(keyspace.getReplication().get("class"));
		}
	}

	@Test
	@DisplayName("a value too big for one v5 segment survives both legs of the round trip")
	void testAFrameSplitAcrossSegments() {
		// A protocol v5 segment carries at most 128 KiB - 1, so this blob is split by the driver on its
		// way out and by the listener on its way back, and reassembled by the other end each time. Both
		// halves are the driver's own SegmentBuilder and SegmentToFrameDecoder rather than code written
		// here, which is the reason to prove they are wired up rather than to trust them.
		final var payload = new byte[384 * 1024];
		new Random(20260730L).nextBytes(payload);
		try (final var connected = connect(false)) {
			// Load-bearing: at v4 there are no segments at all, and this test would pass having proved
			// nothing. The session is unpinned, so it negotiates.
			assertEquals("V5", connected.getContext().getProtocolVersion().name());
			connected.execute(KEYSPACE);
			connected.execute("CREATE TABLE harness.big (id int PRIMARY KEY, payload blob)");
			connected.execute(connected.prepare("INSERT INTO harness.big (id, payload) VALUES (?, ?)")
				.bind(1, ByteBuffer.wrap(payload)));

			final var row = connected.execute("SELECT payload FROM harness.big WHERE id = 1").one();

			assertNotNull(row);
			final var returned = row.getByteBuffer("payload");
			assertNotNull(returned);
			final var bytes = new byte[returned.remaining()];
			returned.duplicate().get(bytes);
			assertArrayEquals(payload, bytes);
		}
	}

	@Test
	@DisplayName("a driver with no configuration at all connects and works")
	void testStockConfiguration() {
		// No config loader, no pinned protocol version, no metadata switched off, no raised timeouts -
		// only the contact point and the datacenter, which a driver requires of everybody. This is the
		// v2 target: a service's own production driver, unmodified, talking to SeaStar. The sequence
		// is deliberately every shape a harness uses rather than the shortest thing that proves a
		// connection: a prepare, an execute, a batch, a select, a schema change, and a select after it.
		try (final var connected = CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()))
			.withLocalDatacenter("datacenter1")
			.build()) {
			connected.execute(KEYSPACE);
			connected.execute(TABLE);

			final var insert = connected.prepare("INSERT INTO harness.t (id, name) VALUES (?, ?)");
			connected.execute(insert.bind(3, "three"));
			connected.execute(BatchStatement.newInstance(DefaultBatchType.LOGGED, insert.bind(4, "four"),
				insert.bind(5, "five")));

			// By name rather than in order: rows come back in partition-token order, which is not
			// insertion order and is not something a caller should be asserting on.
			assertEquals(Set.of("three", "four", "five"),
				connected.execute("SELECT name FROM harness.t")
					.all()
					.stream()
					.map(row -> row.getString("name"))
					.collect(Collectors.toSet()));

			connected.execute("ALTER TABLE harness.t ADD extra text");
			connected.execute("UPDATE harness.t SET extra = 'added' WHERE id = 3");

			final var row = connected.execute("SELECT name, extra FROM harness.t WHERE id = 3").one();
			assertNotNull(row);
			assertEquals("three", row.getString("name"));
			assertEquals("added", row.getString("extra"));

			assertTrue(connected.getMetadata()
				.getKeyspace(CqlIdentifier.fromInternal("harness"))
				.isPresent());
		}
	}

	/**
	 * @param schemaMetadata whether to leave the driver's schema metadata on, which is its default
	 *                       and which makes it query all eight {@code system_schema} tables
	 */
	private CqlSession connect(final boolean schemaMetadata) {
		final var loader = DriverConfigLoader.programmaticBuilder()
			.withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, schemaMetadata)
			.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
			.build();

		return CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()))
			.withLocalDatacenter("datacenter1")
			.withConfigLoader(loader)
			.build();
	}
}
