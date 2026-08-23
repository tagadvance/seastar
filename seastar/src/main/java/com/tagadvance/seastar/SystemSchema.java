package com.tagadvance.seastar;

import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jcip.annotations.ThreadSafe;

/**
 * The {@code system_schema} tables, projected from the live model.
 *
 * <p>A driver with schema metadata enabled builds its whole {@code Metadata} by querying
 * {@code system_schema.keyspaces}, {@code .tables}, {@code .columns}, {@code .types},
 * {@code .indexes}, {@code .views}, {@code .functions} and {@code .aggregates}. SeaStar already
 * holds everything those rows describe - {@link SeaStarKeyspace}, {@link SeaStarTable},
 * {@link SeaStarColumn} and {@link SeaStarUserDefinedType} each extend the driver's own metadata
 * interface - so this is a reshaping rather than a second source of truth. {@code system_schema}
 * uses a flatter and more awkward shape than the driver's metadata interfaces do: a column's kind,
 * its position in the key and its clustering order are columns of their own, and its type is a CQL
 * type <em>string</em>.
 *
 * <p><strong>This is a projection, not a keyspace.</strong> There is no {@code system_schema} in the
 * model, {@code SELECT ... FROM system_schema.tables} is not answerable in-process, and
 * {@code session.getMetadata().getKeyspaces()} does not contain it. An in-process user who never
 * starts a server sees exactly what they saw before.
 *
 * <p>Rows are regenerated on every call. There is no volume here to justify a cache, and a cache
 * would go stale the moment a DDL statement ran.
 *
 * <p>{@code views}, {@code functions} and {@code aggregates} are always empty: materialized views,
 * user-defined functions and user-defined aggregates are unsupported by design (see
 * {@code docs/support-matrix.md}), so a keyspace never holds one. They still carry their full column
 * metadata, because an empty result set is still expected to describe its columns.
 */
@ThreadSafe
public final class SystemSchema {

	/**
	 * The name of the keyspace these tables live in on a real node.
	 */
	public static final String KEYSPACE_NAME = "system_schema";

	private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal(KEYSPACE_NAME);

	/**
	 * The values a plain {@code CREATE TABLE} leaves in {@code system_schema.tables} on
	 * {@code cassandra:5.0.8}. SeaStar stores none of them - it has no compaction, no compression and
	 * no bloom filter - so they are reported as the defaults a real node would have written, which is
	 * what the driver parses into {@code TableMetadata#getOptions()}.
	 */
	private static final Map<String, String> DEFAULT_CACHING = Map.of("keys", "ALL",
		"rows_per_partition", "NONE");
	private static final Map<String, String> DEFAULT_COMPACTION = Map.of("class",
		"org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", "max_threshold", "32",
		"min_threshold", "4");
	private static final Map<String, String> DEFAULT_COMPRESSION = Map.of("chunk_length_in_kb", "16",
		"class", "org.apache.cassandra.io.compress.LZ4Compressor");
	/**
	 * {@code isCompactStorage = isSuper || isDense || !isCompound}, so a lone {@code compound} flag
	 * is what tells the driver the table is not compact - which is what
	 * {@link SeaStarTable#isCompactStorage()} already reports. The flag set is also what keeps the
	 * driver off its Cassandra 2.x parsing path, where a missing {@code is_dense} would be a
	 * {@code NullPointerException}.
	 */
	private static final Set<String> TABLE_FLAGS = Set.of("compound");
	/**
	 * The same, for a table holding a counter column. A node writes {@code counter} beside
	 * {@code compound} there; nothing in the driver reads it, but it is what the row says.
	 */
	private static final Set<String> COUNTER_TABLE_FLAGS = Set.of("compound", "counter");

	private static final DataType MAP_OF_TEXT_TO_TEXT = DataTypes.frozenMapOf(DataTypes.TEXT,
		DataTypes.TEXT);
	private static final DataType MAP_OF_TEXT_TO_BLOB = DataTypes.frozenMapOf(DataTypes.TEXT,
		DataTypes.BLOB);
	private static final DataType SET_OF_TEXT = DataTypes.frozenSetOf(DataTypes.TEXT);
	private static final DataType LIST_OF_TEXT = DataTypes.frozenListOf(DataTypes.TEXT);

	private static final List<Column> KEYSPACES = List.of(new Column("keyspace_name", DataTypes.TEXT),
		new Column("durable_writes", DataTypes.BOOLEAN),
		new Column("replication", MAP_OF_TEXT_TO_TEXT));

	private static final List<Column> TABLES = List.of(new Column("keyspace_name", DataTypes.TEXT),
		new Column("table_name", DataTypes.TEXT),
		new Column("additional_write_policy", DataTypes.TEXT),
		new Column("allow_auto_snapshot", DataTypes.BOOLEAN),
		new Column("bloom_filter_fp_chance", DataTypes.DOUBLE),
		new Column("caching", MAP_OF_TEXT_TO_TEXT), new Column("cdc", DataTypes.BOOLEAN),
		new Column("comment", DataTypes.TEXT), new Column("compaction", MAP_OF_TEXT_TO_TEXT),
		new Column("compression", MAP_OF_TEXT_TO_TEXT),
		new Column("crc_check_chance", DataTypes.DOUBLE),
		new Column("dclocal_read_repair_chance", DataTypes.DOUBLE),
		new Column("default_time_to_live", DataTypes.INT),
		new Column("extensions", MAP_OF_TEXT_TO_BLOB), new Column("flags", SET_OF_TEXT),
		new Column("gc_grace_seconds", DataTypes.INT), new Column("id", DataTypes.UUID),
		new Column("incremental_backups", DataTypes.BOOLEAN),
		new Column("max_index_interval", DataTypes.INT), new Column("memtable", DataTypes.TEXT),
		new Column("memtable_flush_period_in_ms", DataTypes.INT),
		new Column("min_index_interval", DataTypes.INT), new Column("read_repair", DataTypes.TEXT),
		new Column("read_repair_chance", DataTypes.DOUBLE),
		new Column("speculative_retry", DataTypes.TEXT));

	private static final List<Column> COLUMNS = List.of(new Column("keyspace_name", DataTypes.TEXT),
		new Column("table_name", DataTypes.TEXT), new Column("column_name", DataTypes.TEXT),
		new Column("clustering_order", DataTypes.TEXT),
		new Column("column_name_bytes", DataTypes.BLOB), new Column("kind", DataTypes.TEXT),
		new Column("position", DataTypes.INT), new Column("type", DataTypes.TEXT));

	private static final List<Column> TYPES = List.of(new Column("keyspace_name", DataTypes.TEXT),
		new Column("type_name", DataTypes.TEXT), new Column("field_names", LIST_OF_TEXT),
		new Column("field_types", LIST_OF_TEXT));

	private static final List<Column> INDEXES = List.of(new Column("keyspace_name", DataTypes.TEXT),
		new Column("table_name", DataTypes.TEXT), new Column("index_name", DataTypes.TEXT),
		new Column("kind", DataTypes.TEXT), new Column("options", MAP_OF_TEXT_TO_TEXT));

	private static final List<Column> VIEWS = List.of(new Column("keyspace_name", DataTypes.TEXT),
		new Column("view_name", DataTypes.TEXT),
		new Column("additional_write_policy", DataTypes.TEXT),
		new Column("allow_auto_snapshot", DataTypes.BOOLEAN),
		new Column("base_table_id", DataTypes.UUID), new Column("base_table_name", DataTypes.TEXT),
		new Column("bloom_filter_fp_chance", DataTypes.DOUBLE),
		new Column("caching", MAP_OF_TEXT_TO_TEXT), new Column("cdc", DataTypes.BOOLEAN),
		new Column("comment", DataTypes.TEXT), new Column("compaction", MAP_OF_TEXT_TO_TEXT),
		new Column("compression", MAP_OF_TEXT_TO_TEXT),
		new Column("crc_check_chance", DataTypes.DOUBLE),
		new Column("dclocal_read_repair_chance", DataTypes.DOUBLE),
		new Column("default_time_to_live", DataTypes.INT),
		new Column("extensions", MAP_OF_TEXT_TO_BLOB),
		new Column("gc_grace_seconds", DataTypes.INT), new Column("id", DataTypes.UUID),
		new Column("include_all_columns", DataTypes.BOOLEAN),
		new Column("incremental_backups", DataTypes.BOOLEAN),
		new Column("max_index_interval", DataTypes.INT), new Column("memtable", DataTypes.TEXT),
		new Column("memtable_flush_period_in_ms", DataTypes.INT),
		new Column("min_index_interval", DataTypes.INT), new Column("read_repair", DataTypes.TEXT),
		new Column("read_repair_chance", DataTypes.DOUBLE),
		new Column("speculative_retry", DataTypes.TEXT),
		new Column("where_clause", DataTypes.TEXT));

	private static final List<Column> FUNCTIONS = List.of(new Column("keyspace_name", DataTypes.TEXT),
		new Column("function_name", DataTypes.TEXT), new Column("argument_types", LIST_OF_TEXT),
		new Column("argument_names", LIST_OF_TEXT), new Column("body", DataTypes.TEXT),
		new Column("called_on_null_input", DataTypes.BOOLEAN),
		new Column("language", DataTypes.TEXT), new Column("return_type", DataTypes.TEXT));

	private static final List<Column> AGGREGATES = List.of(
		new Column("keyspace_name", DataTypes.TEXT), new Column("aggregate_name", DataTypes.TEXT),
		new Column("argument_types", LIST_OF_TEXT), new Column("final_func", DataTypes.TEXT),
		new Column("initcond", DataTypes.TEXT), new Column("return_type", DataTypes.TEXT),
		new Column("state_func", DataTypes.TEXT), new Column("state_type", DataTypes.TEXT));

	private static final Map<String, Projection> PROJECTIONS = Map.of("keyspaces",
		new Projection(KEYSPACES, SystemSchema::keyspaceRows), "tables",
		new Projection(TABLES, SystemSchema::tableRows), "columns",
		new Projection(COLUMNS, SystemSchema::columnRows), "types",
		new Projection(TYPES, SystemSchema::typeRows), "indexes",
		new Projection(INDEXES, SystemSchema::indexRows), "views",
		new Projection(VIEWS, SystemSchema::noRows), "functions",
		new Projection(FUNCTIONS, SystemSchema::noRows), "aggregates",
		new Projection(AGGREGATES, SystemSchema::noRows));

	private SystemSchema() {
	}

	/**
	 * The rows of one {@code system_schema} table, as they stand right now.
	 *
	 * @param context the model to project
	 * @param table   the unqualified table name, as it appears after {@code system_schema.} - one of
	 *                {@code keyspaces}, {@code tables}, {@code columns}, {@code types},
	 *                {@code indexes}, {@code views}, {@code functions}, {@code aggregates}
	 * @return the projected rows, or {@link Optional#empty()} if {@code system_schema} holds no table
	 * of that name - which a caller serving the wire should answer the way a node answers an
	 * unconfigured table
	 */
	public static Optional<AsyncResultSet> select(final SeaStarDriverContext context,
		final String table) {
		requireNonNull(context, "context must not be null");
		requireNonNull(table, "table must not be null");

		final var projection = PROJECTIONS.get(table);
		if (projection == null) {
			return Optional.empty();
		}

		// The context's read lock, so a keyspace cannot be dropped part way through the projection.
		// Each keyspace's own getters take that keyspace's read lock as they are called, which is the
		// order the hierarchy requires; see AGENTS.md.
		final var rows = context.readLockUnchecked(() -> projection.rows().apply(context));

		return Optional.of(resultSet(context, table, projection.columns(), rows));
	}

	private static List<List<Object>> keyspaceRows(final SeaStarDriverContext context) {
		return keyspaces(context).map(
				keyspace -> row(keyspace.name().asInternal(), keyspace.isDurableWrites(),
					keyspace.getReplication()))
			.toList();
	}

	private static List<List<Object>> tableRows(final SeaStarDriverContext context) {
		final List<List<Object>> rows = new ArrayList<>();
		keyspaces(context).forEach(keyspace -> tables(keyspace).forEach(
			table -> rows.add(row(keyspace.name().asInternal(), table.getName().asInternal(), "99p",
				null, 0.01d, DEFAULT_CACHING, null, "", DEFAULT_COMPACTION, DEFAULT_COMPRESSION, 1.0d,
				0.0d, 0, Map.<String, ByteBuffer>of(), flags(table), 864000, table.getId().orElse(null),
				null, 2048, null, 0, 128, "BLOCKING", 0.0d, "99p"))));

		return rows;
	}

	private static List<List<Object>> columnRows(final SeaStarDriverContext context) {
		final List<List<Object>> rows = new ArrayList<>();
		keyspaces(context).forEach(keyspace -> tables(keyspace).forEach(table -> {
			final var partitionKey = table.getPartitionKey()
				.stream()
				.map(ColumnMetadata::getName)
				.toList();
			final Map<ColumnMetadata, ClusteringOrder> clusteringColumns =
				table.getClusteringColumns();
			final var clustering = clusteringColumns.keySet()
				.stream()
				.map(ColumnMetadata::getName)
				.toList();
			final var orders = List.copyOf(clusteringColumns.values());

			table.getColumns()
				.values()
				.stream()
				.sorted(comparing(column -> column.getName().asInternal()))
				.forEach(column -> {
					final var name = column.getName();
					final var partitionKeyIndex = partitionKey.indexOf(name);
					final var clusteringIndex = clustering.indexOf(name);
					final String kind;
					final int position;
					if (partitionKeyIndex >= 0) {
						kind = "partition_key";
						position = partitionKeyIndex;
					} else if (clusteringIndex >= 0) {
						kind = "clustering";
						position = clusteringIndex;
					} else {
						kind = column.isStatic() ? "static" : "regular";
						position = -1;
					}
					final var order = clusteringIndex < 0 ? "none"
						: orders.get(clusteringIndex).name().toLowerCase(Locale.ROOT);

					rows.add(row(keyspace.name().asInternal(), table.getName().asInternal(),
						name.asInternal(), order, nameBytes(name), kind, position,
						cqlType(column.getType())));
				});
		}));

		return rows;
	}

	private static List<List<Object>> typeRows(final SeaStarDriverContext context) {
		final List<List<Object>> rows = new ArrayList<>();
		keyspaces(context).forEach(keyspace -> keyspace.getSeaStarUserDefinedTypes()
			.values()
			.stream()
			.sorted(comparing(type -> type.getName().asInternal()))
			.forEach(type -> rows.add(row(keyspace.name().asInternal(), type.getName().asInternal(),
				type.getFieldNames().stream().map(CqlIdentifier::asInternal).toList(),
				type.getFieldTypes().stream().map(SystemSchema::cqlType).toList()))));

		return rows;
	}

	private static List<List<Object>> indexRows(final SeaStarDriverContext context) {
		final List<List<Object>> rows = new ArrayList<>();
		keyspaces(context).forEach(keyspace -> tables(keyspace).forEach(table -> table.getIndexes()
			.values()
			.stream()
			.sorted(comparing(index -> index.getName().asInternal()))
			.forEach(index -> rows.add(
				row(keyspace.name().asInternal(), table.getName().asInternal(),
					index.getName().asInternal(), index.getKind().name(), index.getOptions())))));

		return rows;
	}

	/**
	 * @param context ignored; present so that an always-empty table is the same shape of function as
	 *                a projected one
	 */
	@SuppressWarnings("unused")
	private static List<List<Object>> noRows(final SeaStarDriverContext context) {
		return List.of();
	}

	/**
	 * The keyspaces of the model, by name. A real node returns partitions in token order, which is
	 * arbitrary from the outside; the driver re-keys every row by name and never looks at the order,
	 * so a stable one is worth more here than an imitated one.
	 */
	private static Stream<SeaStarKeyspace> keyspaces(final SeaStarDriverContext context) {
		return context.getSeaStarKeyspaces()
			.values()
			.stream()
			.sorted(comparing(keyspace -> keyspace.name().asInternal()));
	}

	/**
	 * The tables of a keyspace, in the order a real node clusters them - {@code table_name ASC}.
	 */
	private static Stream<SeaStarTable> tables(final SeaStarKeyspace keyspace) {
		return keyspace.getSeaStarTables()
			.values()
			.stream()
			.sorted(comparing(table -> table.getName().asInternal()));
	}

	/**
	 * The flags a node writes for a table: {@code compound} always, plus {@code counter} when the
	 * table holds a counter column.
	 */
	private static Set<String> flags(final SeaStarTable table) {
		final var isCounter = table.getColumns()
			.values()
			.stream()
			.anyMatch(column -> DataTypes.COUNTER.equals(column.getType()));

		return isCounter ? COUNTER_TABLE_FLAGS : TABLE_FLAGS;
	}

	private static ByteBuffer nameBytes(final CqlIdentifier name) {
		return ByteBuffer.wrap(name.asInternal().getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * A row's values. {@link Arrays#asList} rather than {@link List#of} because several columns are
	 * legitimately null - {@code cdc} and {@code memtable} are null on a real node unless they have
	 * been set - and {@code List.of} refuses a null element.
	 */
	private static List<Object> row(final Object... values) {
		return Arrays.asList(values);
	}

	/**
	 * A type as {@code system_schema} spells it.
	 *
	 * <p>This is deliberately not {@link DataType#asCql(boolean, boolean)}. That method qualifies a
	 * user-defined type with its keyspace ({@code frozen<ks.address>}), and {@code system_schema}
	 * does not: a type is looked up in the keyspace whose row it appears in, and the driver's
	 * {@code DataTypeCqlNameParser} resolves the bare name against that keyspace's type map. A
	 * qualified name there is an {@code IllegalStateException} inside the driver's schema parser, so
	 * the recursion is written out rather than delegated.
	 */
	private static String cqlType(final DataType type) {
		if (type instanceof UserDefinedType userDefinedType) {
			return frozen(userDefinedType.isFrozen(), userDefinedType.getName().asCql(true));
		} else if (type instanceof ListType list) {
			return frozen(list.isFrozen(), "list<%s>".formatted(cqlType(list.getElementType())));
		} else if (type instanceof SetType set) {
			return frozen(set.isFrozen(), "set<%s>".formatted(cqlType(set.getElementType())));
		} else if (type instanceof MapType map) {
			return frozen(map.isFrozen(),
				"map<%s, %s>".formatted(cqlType(map.getKeyType()), cqlType(map.getValueType())));
		} else if (type instanceof TupleType tuple) {
			// A tuple is always frozen, and Cassandra writes the keyword out.
			return frozen(true, "tuple<%s>".formatted(
				tuple.getComponentTypes().stream().map(SystemSchema::cqlType).collect(joining(", "))));
		}

		return type.asCql(true, false);
	}

	private static String frozen(final boolean isFrozen, final String type) {
		return isFrozen ? "frozen<%s>".formatted(type) : type;
	}

	/**
	 * The rows as a result set, built out of the ordinary model: a keyspace and a table that are
	 * never registered with the context, so nothing else can see them and
	 * {@code getMetadata().getKeyspaces()} is unchanged. Going through {@link VolatileTable} rather
	 * than a bespoke {@code Row} is what gives the result its column definitions, its codec-checked
	 * values and its {@code system_schema} keyspace and table names in the column metadata.
	 */
	private static AsyncResultSet resultSet(final SeaStarDriverContext context, final String table,
		final List<Column> columns, final List<List<Object>> values) {
		final var keyspace = new VolatileKeyspace(context, KEYSPACE, Map.of(),
			SeaStarDriverContext.DEFAULT_DURABLE_WRITES);
		final var projection = keyspace.newSeaStarTable(CqlIdentifier.fromInternal(table));
		columns.forEach(
			column -> projection.addColumn(CqlIdentifier.fromInternal(column.name()), column.type()));
		values.forEach(projection::addRow);

		final var statement = SimpleStatement.newInstance(
			"SELECT * FROM %s.%s".formatted(KEYSPACE_NAME, table));
		final var executionInfo = new SeaStarExecutionInfo(context.getNode(), statement);

		return new SeaStarAsyncResultSet(projection.snapshot(), executionInfo,
			projection.rows()
				.map(SeaStarRow::snapshot)
				.collect(Collectors.toCollection(LinkedList::new)));
	}

	private record Column(String name, DataType type) {

	}

	private record Projection(List<Column> columns,
							  Function<SeaStarDriverContext, List<List<Object>>> rows) {

	}

}
