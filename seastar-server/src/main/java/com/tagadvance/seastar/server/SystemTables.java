package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * {@code system.local}, {@code system.peers} and {@code system.peers_v2}, as this server describes
 * itself.
 *
 * <p>A driver does not finish connecting until these answer. {@code ProtocolInitHandler} reads the
 * cluster name out of {@code system.local} as the step straight after {@code READY}, and
 * {@code DefaultTopologyMonitor} builds the driver's whole idea of the node - its datacenter, its
 * rack, its tokens, its host id - from the same row. A column it reads that is missing is a
 * {@code NullPointerException} inside driver internals rather than a legible error, so the column
 * list here is the one a {@code cassandra:5.0.8} container carries, not a subset.
 *
 * <p><strong>These are not a projection of the model, and there is deliberately no {@code system}
 * keyspace in it.</strong> Every value below is a fact about the listener rather than about the
 * data, which is why they live in this module: an in-process user who never starts a server sees no
 * invented keyspaces in {@code getMetadata().getKeyspaces()}. Contrast {@code SystemSchema}, which
 * genuinely is a projection and therefore lives in the core.
 *
 * <p>Values chosen so they are internally consistent rather than merely present:
 *
 * <ul>
 *   <li>{@code partitioner} is Murmur3, which is not a lie - the core's {@code Tokens} already
 *       hashes a partition key the way {@code Murmur3Partitioner} does, and it is what a
 *       token-aware driver builds its ring from.</li>
 *   <li>{@code tokens} is the one token that owns the whole ring, {@code Long.MIN_VALUE}. It has to
 *       be non-empty: a node owning no range is a node a load balancing policy may skip.</li>
 *   <li>{@code host_id} is generated once per server and never changes, because the driver keys its
 *       node map by it.</li>
 *   <li>{@code release_version} matches the {@code cassandra-all} pin. Drivers gate features on it,
 *       and claiming a version whose features SeaStar lacks is the wrong direction to be wrong
 *       in.</li>
 *   <li>{@code schema_version} is a real UUID that changes when a DDL statement runs - see
 *       {@link #schemaChanged()}.</li>
 * </ul>
 *
 * <p>{@code system.peers} and {@code system.peers_v2} are always empty. One node has no peers, and
 * a real Cassandra 4 or 5 node has both tables, so there is nothing to emulate about the driver's
 * fallback from one to the other. They still carry their full column metadata, because an empty
 * result set is still expected to describe its columns.
 */
@ThreadSafe
final class SystemTables {

	/**
	 * The name of the keyspace most of these tables live in on a real node.
	 */
	static final String KEYSPACE_NAME = "system";

	/**
	 * The keyspace a Cassandra 4 or 5 node describes its <em>virtual</em> tables in. SeaStar has
	 * none, so all three of its tables are empty - but they have to answer rather than fail, because
	 * the driver runs them alongside the {@code system_schema} queries in one batch and one failure
	 * abandons the whole schema refresh with "Unexpected error while refreshing schema".
	 */
	static final String VIRTUAL_SCHEMA_KEYSPACE_NAME = "system_virtual_schema";

	/**
	 * The core computes Murmur3 tokens already, so this is what SeaStar actually partitions by.
	 */
	private static final String PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";

	/**
	 * The {@code cassandra-all} pin, so that a driver gating a feature on the server version gets
	 * the same answer it would from the container the fidelity suite runs against.
	 */
	private static final String RELEASE_VERSION = "5.0.8";

	/** What {@code OPTIONS} already advertises; captured from {@code cassandra:5.0.8}. */
	private static final String CQL_VERSION = "3.4.7";

	/** The one version this server serves. */
	private static final String NATIVE_PROTOCOL_VERSION = "4";

	/**
	 * The storage port a real node gossips on. SeaStar has no storage port and nothing listens here;
	 * the driver only ever assembles it into a {@code broadcast_address} it reports back, so the
	 * container's value is more honest than inventing one.
	 */
	private static final int STORAGE_PORT = 7000;

	/**
	 * One token, the minimum, so this node owns the whole ring. Any plausible token would do with a
	 * single node; an empty set would not.
	 */
	private static final Set<String> TOKENS = Set.of(Long.toString(Long.MIN_VALUE));

	private static final DataType SET_OF_TEXT = DataTypes.setOf(DataTypes.TEXT);
	private static final DataType MAP_OF_UUID_TO_BLOB = DataTypes.mapOf(DataTypes.UUID,
		DataTypes.BLOB);

	private static final List<Column> LOCAL = List.of(new Column("key", DataTypes.TEXT),
		new Column("bootstrapped", DataTypes.TEXT), new Column("broadcast_address", DataTypes.INET),
		new Column("broadcast_port", DataTypes.INT), new Column("cluster_name", DataTypes.TEXT),
		new Column("cql_version", DataTypes.TEXT), new Column("data_center", DataTypes.TEXT),
		new Column("gossip_generation", DataTypes.INT), new Column("host_id", DataTypes.UUID),
		new Column("listen_address", DataTypes.INET), new Column("listen_port", DataTypes.INT),
		new Column("native_protocol_version", DataTypes.TEXT),
		new Column("partitioner", DataTypes.TEXT), new Column("rack", DataTypes.TEXT),
		new Column("release_version", DataTypes.TEXT), new Column("rpc_address", DataTypes.INET),
		new Column("rpc_port", DataTypes.INT), new Column("schema_version", DataTypes.UUID),
		new Column("tokens", SET_OF_TEXT), new Column("truncated_at", MAP_OF_UUID_TO_BLOB));

	private static final List<Column> PEERS = List.of(new Column("peer", DataTypes.INET),
		new Column("data_center", DataTypes.TEXT), new Column("host_id", DataTypes.UUID),
		new Column("preferred_ip", DataTypes.INET), new Column("rack", DataTypes.TEXT),
		new Column("release_version", DataTypes.TEXT), new Column("rpc_address", DataTypes.INET),
		new Column("schema_version", DataTypes.UUID), new Column("tokens", SET_OF_TEXT));

	private static final List<Column> VIRTUAL_KEYSPACES = List.of(
		new Column("keyspace_name", DataTypes.TEXT));

	private static final List<Column> VIRTUAL_TABLES = List.of(
		new Column("keyspace_name", DataTypes.TEXT), new Column("table_name", DataTypes.TEXT),
		new Column("comment", DataTypes.TEXT));

	private static final List<Column> VIRTUAL_COLUMNS = List.of(
		new Column("keyspace_name", DataTypes.TEXT), new Column("table_name", DataTypes.TEXT),
		new Column("column_name", DataTypes.TEXT), new Column("clustering_order", DataTypes.TEXT),
		new Column("column_name_bytes", DataTypes.BLOB), new Column("kind", DataTypes.TEXT),
		new Column("position", DataTypes.INT), new Column("type", DataTypes.TEXT));

	private static final List<Column> PEERS_V2 = List.of(new Column("peer", DataTypes.INET),
		new Column("peer_port", DataTypes.INT), new Column("data_center", DataTypes.TEXT),
		new Column("host_id", DataTypes.UUID), new Column("native_address", DataTypes.INET),
		new Column("native_port", DataTypes.INT), new Column("preferred_ip", DataTypes.INET),
		new Column("preferred_port", DataTypes.INT), new Column("rack", DataTypes.TEXT),
		new Column("release_version", DataTypes.TEXT), new Column("schema_version", DataTypes.UUID),
		new Column("tokens", SET_OF_TEXT));

	private final String clusterName;
	private final String datacenter;
	private final String rack;
	private final InetAddress address;
	private final IntSupplier port;
	private final UUID hostId = UUID.randomUUID();
	private final AtomicReference<UUID> schemaVersion = new AtomicReference<>(UUID.randomUUID());

	/**
	 * @param clusterName the cluster name to report
	 * @param datacenter  the datacenter to report
	 * @param rack        the rack to report
	 * @param address     the address the server is bound to
	 * @param port        the port the server is bound to, read late because an ephemeral port is not
	 *                    known until the bind has happened
	 */
	SystemTables(final String clusterName, final String datacenter, final String rack,
		final InetAddress address, final IntSupplier port) {
		this.clusterName = clusterName;
		this.datacenter = datacenter;
		this.rack = rack;
		this.address = address;
		this.port = port;
	}

	/**
	 * Moves the schema version on, as a real node does when its schema changes.
	 *
	 * <p>It has to be a change rather than a constant, because a driver treats a version it has not
	 * seen before as the cue to refresh its metadata. It has to be stable between changes for the
	 * opposite reason: after any DDL statement the driver compares {@code system.local}'s version
	 * against every peer's until they agree, and a version that moved under it would never agree and
	 * would cost the full ten-second timeout on every DDL statement in a harness.
	 */
	void schemaChanged() {
		schemaVersion.set(UUID.randomUUID());
	}

	/**
	 * @param keyspace a keyspace named by a query
	 * @return whether this is one of the keyspaces answered here
	 */
	static boolean answers(final String keyspace) {
		return KEYSPACE_NAME.equals(keyspace) || VIRTUAL_SCHEMA_KEYSPACE_NAME.equals(keyspace);
	}

	/**
	 * @param query a select against one of the keyspaces {@link #answers} claims
	 * @return the rows it asked for, or the error a node answers a table it does not have with
	 */
	Message select(final SystemQuery query) {
		final var table = query.table();
		final var columns = columns(query.keyspace(), table);
		if (columns == null) {
			return new Error(ProtocolConstants.ErrorCode.INVALID,
				"table " + table + " does not exist");
		}

		// system.local is the only one of these with anything in it: one node has no peers, and
		// SeaStar has no virtual tables.
		final var rows = KEYSPACE_NAME.equals(query.keyspace()) && "local".equals(table)
			? List.of(local()) : List.<List<Object>>of();

		return query.project(rows(query.keyspace(), table, columns, rows));
	}

	private static @Nullable List<Column> columns(final String keyspace, final String table) {
		if (KEYSPACE_NAME.equals(keyspace)) {
			return switch (table) {
				case "local" -> LOCAL;
				case "peers" -> PEERS;
				case "peers_v2" -> PEERS_V2;
				default -> null;
			};
		}

		return switch (table) {
			case "keyspaces" -> VIRTUAL_KEYSPACES;
			case "tables" -> VIRTUAL_TABLES;
			case "columns" -> VIRTUAL_COLUMNS;
			default -> null;
		};
	}

	/**
	 * The one row of {@code system.local}, in the column order a {@code SELECT *} returns it in on a
	 * real node.
	 */
	private List<Object> local() {
		return Arrays.asList("local", "COMPLETED", address, STORAGE_PORT, clusterName, CQL_VERSION,
			datacenter, 0, hostId, address, STORAGE_PORT, NATIVE_PROTOCOL_VERSION, PARTITIONER, rack,
			RELEASE_VERSION, address, port.getAsInt(), schemaVersion.get(), TOKENS, null);
	}

	/**
	 * Rows as the protocol sends them. The values go through the driver's own codec for the column's
	 * type - the same codec the driver decodes them with at the other end - so there is no second
	 * opinion about the encoding anywhere in this module.
	 */
	private static Message rows(final String keyspace, final String table,
		final List<Column> columns, final List<List<Object>> values) {
		final var specs = new ArrayList<ColumnSpec>(columns.size());
		for (int i = 0; i < columns.size(); i++) {
			final var column = columns.get(i);
			specs.add(new ColumnSpec(keyspace, table, column.name(), i, RawTypes.of(column.type())));
		}

		final var data = new ArrayDeque<List<ByteBuffer>>(values.size());
		for (final var value : values) {
			final var encoded = new ArrayList<ByteBuffer>(columns.size());
			for (int i = 0; i < columns.size(); i++) {
				encoded.add(encode(columns.get(i).type(), value.get(i)));
			}
			data.add(encoded);
		}

		// No paging state: one page, always, which is what says "there is no more" to a driver.
		return new DefaultRows(new RowsMetadata(specs, null, null, null), data);
	}

	private static @Nullable ByteBuffer encode(final DataType type, final @Nullable Object value) {
		if (value == null) {
			return null;
		}

		final TypeCodec<Object> codec = CodecRegistry.DEFAULT.codecFor(type);

		return codec.encode(value, DefaultProtocolVersion.V4);
	}

	private record Column(String name, DataType type) {

	}

}
