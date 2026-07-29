package com.tagadvance.seastar.server;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.tagadvance.seastar.SeaStarCqlSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * d_plan D1, D2, D3 and D5 driven off the wire: the exact query strings java-driver-core 4.19.3
 * sends on its control connection, and the rows that come back.
 *
 * <p>The column lists and their types were taken from {@code DESCRIBE TABLE} on a
 * {@code cassandra:5.0.8} container, not written from memory. What each column has to <em>contain</em>
 * comes from the other authority - {@code DefaultTopologyMonitor#nodeInfoBuilder},
 * {@code SchemaAgreementChecker} and {@code PeerRowValidator} in the driver - and
 * {@link DriverSessionTest} is where a real session proves the two agree.
 */
class WireSystemTableTest {

	private static final int V4 = ProtocolConstants.Version.V4;

	/** The literal queries the driver sends; see {@link SystemQuery}. */
	private static final String CLUSTER_NAME = "SELECT cluster_name FROM system.local";
	private static final String LOCAL = "SELECT * FROM system.local";
	private static final String PEERS = "SELECT * FROM system.peers";
	private static final String PEERS_V2 = "SELECT * FROM system.peers_v2";
	private static final String SCHEMA_VERSION =
		"SELECT schema_version FROM system.local WHERE key='local'";

	private final AtomicInteger streamIds = new AtomicInteger();

	private SeaStarCqlSession session;
	private SeaStarProtocolServer server;
	private WireClient client;

	@BeforeEach
	void setUp() throws IOException {
		session = SeaStarCqlSession.builder().build();
		server = SeaStarProtocolServer.builder().session(session).build().start();
		client = new WireClient(server.port());
		client.send(V4, streamIds.incrementAndGet(), new Startup());
	}

	@AfterEach
	void tearDown() throws IOException {
		client.close();
		server.close();
		session.close();
	}

	@Test
	@DisplayName("system.local carries every column a cassandra:5.0.8 node has, in its order")
	void testLocalColumns() throws IOException {
		final var rows = rows(LOCAL);

		assertEquals(
			List.of("key", "bootstrapped", "broadcast_address", "broadcast_port", "cluster_name",
				"cql_version", "data_center", "gossip_generation", "host_id", "listen_address",
				"listen_port", "native_protocol_version", "partitioner", "rack", "release_version",
				"rpc_address", "rpc_port", "schema_version", "tokens", "truncated_at"),
			names(rows.getMetadata().columnSpecs));
		assertEquals(1, rows.getData().size());
		assertEquals("system", rows.getMetadata().columnSpecs.get(0).ksName);
		assertEquals("local", rows.getMetadata().columnSpecs.get(0).tableName);
	}

	@Test
	@DisplayName("system.local describes a node the driver can build a NodeInfo from")
	void testLocalValues() throws IOException {
		final var rows = rows(LOCAL);

		assertEquals("local", value(rows, "key", TypeCodecs.TEXT));
		assertEquals("SeaStar", value(rows, "cluster_name", TypeCodecs.TEXT));
		assertEquals("datacenter1", value(rows, "data_center", TypeCodecs.TEXT));
		assertEquals("rack1", value(rows, "rack", TypeCodecs.TEXT));
		assertEquals("5.0.8", value(rows, "release_version", TypeCodecs.TEXT));
		assertEquals("org.apache.cassandra.dht.Murmur3Partitioner",
			value(rows, "partitioner", TypeCodecs.TEXT));
		assertEquals(server.port(), value(rows, "rpc_port", TypeCodecs.INT));
		assertNotNull(value(rows, "host_id", TypeCodecs.UUID));
		assertNotNull(value(rows, "schema_version", TypeCodecs.UUID));
		assertNotNull(value(rows, "rpc_address", TypeCodecs.INET));

		// A node that owns no range is a node a load balancing policy may decide to skip, so the set
		// being non-empty is the thing worth asserting.
		final Set<String> tokens = value(rows, "tokens", TypeCodecs.setOf(TypeCodecs.TEXT));
		assertNotNull(tokens);
		assertFalse(tokens.isEmpty());
		tokens.forEach(Long::parseLong);
	}

	@Test
	@DisplayName("the datacenter, rack and cluster name are what the builder was told")
	void testConfiguredIdentity() throws IOException {
		try (final var configured = SeaStarProtocolServer.builder()
			.session(session)
			.clusterName("harness")
			.datacenter("dc7")
			.rack("r9")
			.build()
			.start(); final var other = new WireClient(configured.port())) {
			other.send(V4, 1, new Startup());
			final var rows = assertInstanceOf(Rows.class, other.send(V4, 2, new Query(LOCAL)).message);

			assertEquals("harness", value(rows, "cluster_name", TypeCodecs.TEXT));
			assertEquals("dc7", value(rows, "data_center", TypeCodecs.TEXT));
			assertEquals("r9", value(rows, "rack", TypeCodecs.TEXT));
		}
	}

	@Test
	@DisplayName("the cluster-name query is narrowed to the one column it selected")
	void testProjection() throws IOException {
		final var rows = rows(CLUSTER_NAME);

		// ProtocolInitHandler reads the cluster name out of column 0 positionally, so answering with
		// the whole of system.local would have it believe the cluster is called "local".
		assertEquals(List.of("cluster_name"), names(rows.getMetadata().columnSpecs));
		assertEquals("SeaStar", value(rows, "cluster_name", TypeCodecs.TEXT));
	}

	@Test
	@DisplayName("a column system.local does not have is an INVALID naming it")
	void testUndefinedColumn() throws IOException {
		final var error = assertInstanceOf(Error.class,
			send("SELECT nonesuch FROM system.local"));

		assertEquals(ProtocolConstants.ErrorCode.INVALID, error.code);
		assertTrue(error.message.contains("nonesuch"), error.message);
	}

	@Test
	@DisplayName("both peers tables are empty but still describe their columns")
	void testPeersAreEmpty() throws IOException {
		final var peers = rows(PEERS);
		final var peersV2 = rows(PEERS_V2);

		assertEquals(0, peers.getData().size());
		assertEquals(0, peersV2.getData().size());
		assertEquals(
			List.of("peer", "data_center", "host_id", "preferred_ip", "rack", "release_version",
				"rpc_address", "schema_version", "tokens"), names(peers.getMetadata().columnSpecs));
		assertEquals(
			List.of("peer", "peer_port", "data_center", "host_id", "native_address", "native_port",
				"preferred_ip", "preferred_port", "rack", "release_version", "schema_version",
				"tokens"), names(peersV2.getMetadata().columnSpecs));
	}

	@Test
	@DisplayName("a table the system keyspace does not have is refused the way a node refuses one")
	void testUnknownSystemTable() throws IOException {
		final var error = assertInstanceOf(Error.class, send("SELECT * FROM system.size_estimates"));

		assertEquals(ProtocolConstants.ErrorCode.INVALID, error.code);
		assertTrue(error.message.contains("size_estimates"), error.message);
	}

	@Test
	@DisplayName("the schema-agreement query answers a stable version until a DDL statement runs")
	void testSchemaVersionMovesOnlyOnDdl() throws IOException {
		final var before = schemaVersion();

		assertEquals(before, schemaVersion(), "a version that moves on its own never agrees");

		send("CREATE KEYSPACE d5 WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}");
		final var after = schemaVersion();

		assertNotEquals(before, after);
		assertEquals(after, schemaVersion());
	}

	@Test
	@DisplayName("system_schema is answered from the core's projection")
	void testSystemSchemaIsServed() throws IOException {
		send("CREATE KEYSPACE d1 WITH replication = "
			+ "{'class':'SimpleStrategy','replication_factor':1}");

		final var rows = rows("SELECT * FROM system_schema.keyspaces");

		assertEquals(List.of("keyspace_name", "durable_writes", "replication"),
			names(rows.getMetadata().columnSpecs));
		assertEquals("d1", value(rows, "keyspace_name", TypeCodecs.TEXT));
	}

	@Test
	@DisplayName("system_virtual_schema answers empty rather than failing, which unblocks a refresh")
	void testVirtualSchemaIsEmpty() throws IOException {
		// A Cassandra 4+ node has these, and the driver runs them in the same batch as the eight
		// system_schema queries: one failure abandons the whole refresh, so an error here costs a
		// stock-configured session its metadata entirely.
		final var keyspaces = rows("SELECT * FROM system_virtual_schema.keyspaces");

		assertEquals(List.of("keyspace_name"), names(keyspaces.getMetadata().columnSpecs));
		assertEquals(0, keyspaces.getData().size());
		assertEquals(0, rows("SELECT * FROM system_virtual_schema.tables").getData().size());
		assertEquals(0, rows("SELECT * FROM system_virtual_schema.columns").getData().size());
	}

	@Test
	@DisplayName("a table system_schema does not have is refused the way a node refuses one")
	void testUnknownSystemSchemaTable() throws IOException {
		final var error = assertInstanceOf(Error.class, send("SELECT * FROM system_schema.edges"));

		assertEquals(ProtocolConstants.ErrorCode.INVALID, error.code);
		assertTrue(error.message.contains("edges"), error.message);
	}

	@Test
	@DisplayName("a system query can be prepared and executed, and never reaches the session")
	void testPrepareAndExecute() throws IOException {
		final var prepared = assertInstanceOf(Prepared.class,
			send(new Prepare(LOCAL)));

		assertEquals(0, prepared.variablesMetadata.columnCount);
		assertEquals(20, prepared.resultMetadata.columnSpecs.size());

		final var rows = assertInstanceOf(Rows.class,
			send(new Execute(prepared.preparedQueryId, QueryOptions.DEFAULT)));

		assertEquals(1, rows.getData().size());
		assertEquals("local", value(rows, "key", TypeCodecs.TEXT));
	}

	@Test
	@DisplayName("preparing the same system query twice hands back the same id")
	void testPreparedIdIsStable() throws IOException {
		final var first = assertInstanceOf(Prepared.class, send(new Prepare(PEERS)));
		final var second = assertInstanceOf(Prepared.class, send(new Prepare(PEERS)));

		assertArrayEquals(first.preparedQueryId, second.preparedQueryId);
	}

	@Test
	@DisplayName("an unqualified select is the model's, whatever table it names")
	void testUnqualifiedSelectIsNotIntercepted() throws IOException {
		final var error = assertInstanceOf(Error.class, send("SELECT * FROM local"));

		assertEquals(ProtocolConstants.ErrorCode.INVALID, error.code);
		assertTrue(error.message.contains("keyspace"), error.message);
	}

	private UUID schemaVersion() throws IOException {
		return value(rows(SCHEMA_VERSION), "schema_version", TypeCodecs.UUID);
	}

	private Rows rows(final String query) throws IOException {
		return assertInstanceOf(Rows.class, send(query));
	}

	private Message send(final String query) throws IOException {
		return send(new Query(query));
	}

	private Message send(final Message request) throws IOException {
		return client.send(V4, streamIds.incrementAndGet(), request).message;
	}

	private static List<String> names(final List<ColumnSpec> specs) {
		return specs.stream().map(spec -> spec.name).toList();
	}

	/**
	 * The value of one column of the first row, decoded with the driver's codec for it - the same
	 * codec {@code AdminRow} reads these rows with.
	 */
	private static <T> T value(final Rows rows, final String name, final TypeCodec<T> codec) {
		final var index = names(rows.getMetadata().columnSpecs).indexOf(name);
		assertTrue(index >= 0, name);
		final ByteBuffer value = rows.getData().peek().get(index);

		return codec.decode(value, DefaultProtocolVersion.V4);
	}

}
