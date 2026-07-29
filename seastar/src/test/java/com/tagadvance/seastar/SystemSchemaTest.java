package com.tagadvance.seastar;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeCqlNameParser;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The {@code system_schema} projection, checked against what a real {@code cassandra:5.0.8} node
 * writes into those tables.
 *
 * <p>Every expectation here was read off a live node rather than out of the documentation. The
 * schema below is the one that was created on it: a composite partition key, clustering columns in
 * both directions, a static column, three collections, a user-defined type used both directly and
 * as a collection element, and a secondary index. The driver parses these rows strictly, so a
 * projection that is merely plausible is a projection that fails at connect time.
 */
class SystemSchemaTest {

	private SeaStarCqlSession session;
	private SeaStarDriverContext context;

	@BeforeEach
	void beforeEach() {
		session = SeaStarCqlSession.builder().build();
		context = session.getContext();

		session.execute("CREATE KEYSPACE d4 WITH replication = "
			+ "{'class': 'SimpleStrategy', 'replication_factor': 1} AND durable_writes = true");
		session.execute("CREATE TYPE d4.address (street text, city text, zip int)");
		session.execute("""
			CREATE TABLE d4.awkward (
			  pk1 text, pk2 int, ck1 timeuuid, ck2 int, st1 text static, reg1 int,
			  tags set<text>, scores map<text, int>, items list<double>,
			  home frozen<address>, places map<text, frozen<address>>,
			  PRIMARY KEY ((pk1, pk2), ck1, ck2)
			) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC)""");
		session.execute("CREATE INDEX awkward_reg1_idx ON d4.awkward (reg1)");
		session.execute("CREATE TABLE d4.simple (id uuid PRIMARY KEY, v text)");
	}

	@AfterEach
	void afterEach() {
		session.close();
	}

	@Test
	@DisplayName("system_schema.keyspaces projects the name, durable writes and replication")
	void testKeyspaces() {
		final var resultSet = select("keyspaces");

		assertEquals(List.of("keyspace_name", "durable_writes", "replication"),
			columnNames(resultSet));

		final var rows = rows(resultSet);
		assertEquals(1, rows.size());
		final var row = rows.get(0);
		assertEquals("d4", row.getString("keyspace_name"));
		assertTrue(row.getBoolean("durable_writes"));
		assertEquals(
			Map.of("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"),
			row.getMap("replication", String.class, String.class));
	}

	@Test
	@DisplayName("system_schema.tables carries the 25 columns a 5.0.8 node returns, in order")
	void testTableColumns() {
		assertEquals(
			List.of("keyspace_name", "table_name", "additional_write_policy", "allow_auto_snapshot",
				"bloom_filter_fp_chance", "caching", "cdc", "comment", "compaction", "compression",
				"crc_check_chance", "dclocal_read_repair_chance", "default_time_to_live", "extensions",
				"flags", "gc_grace_seconds", "id", "incremental_backups", "max_index_interval",
				"memtable", "memtable_flush_period_in_ms", "min_index_interval", "read_repair",
				"read_repair_chance", "speculative_retry"), columnNames(select("tables")));
	}

	@Test
	@DisplayName("system_schema.tables projects one row per table with a real id and compound flags")
	void testTables() {
		final var rows = rows(select("tables"));

		assertEquals(List.of("awkward", "simple"),
			rows.stream().map(row -> row.getString("table_name")).collect(toList()));

		final var awkward = rows.get(0);
		assertEquals("d4", awkward.getString("keyspace_name"));
		// Not compact storage, and the flag set is also what keeps the driver's TableParser off its
		// Cassandra 2.x branch, where a missing is_dense is a NullPointerException.
		assertEquals(Set.of("compound"), awkward.getSet("flags", String.class));
		assertEquals(context.getSeaStarKeyspace("d4")
			.orElseThrow(() -> new IllegalStateException("keyspace d4 is required by this test"))
			.getSeaStarTable("awkward")
			.orElseThrow(() -> new IllegalStateException("table awkward is required by this test"))
			.getId()
			.orElseThrow(() -> new IllegalStateException("a table always has an id")),
			awkward.getUuid("id"));
	}

	@Test
	@DisplayName("system_schema.tables reports the option defaults a plain CREATE TABLE leaves")
	void testTableOptions() {
		final var awkward = rows(select("tables")).get(0);

		assertEquals("99p", awkward.getString("additional_write_policy"));
		assertEquals(0.01d, awkward.getDouble("bloom_filter_fp_chance"));
		assertEquals(Map.of("keys", "ALL", "rows_per_partition", "NONE"),
			awkward.getMap("caching", String.class, String.class));
		assertEquals("", awkward.getString("comment"));
		assertEquals("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
			awkward.getMap("compaction", String.class, String.class).get("class"));
		assertEquals("org.apache.cassandra.io.compress.LZ4Compressor",
			awkward.getMap("compression", String.class, String.class).get("class"));
		assertEquals(1.0d, awkward.getDouble("crc_check_chance"));
		assertEquals(0, awkward.getInt("default_time_to_live"));
		assertEquals(Map.of(), awkward.getMap("extensions", String.class, java.nio.ByteBuffer.class));
		assertEquals(864000, awkward.getInt("gc_grace_seconds"));
		assertEquals(2048, awkward.getInt("max_index_interval"));
		assertEquals(0, awkward.getInt("memtable_flush_period_in_ms"));
		assertEquals(128, awkward.getInt("min_index_interval"));
		assertEquals("BLOCKING", awkward.getString("read_repair"));
		assertEquals("99p", awkward.getString("speculative_retry"));
		// Null on a real node unless the option has actually been set on the table.
		assertNull(awkward.getObject("allow_auto_snapshot"));
		assertNull(awkward.getObject("cdc"));
		assertNull(awkward.getObject("incremental_backups"));
		assertNull(awkward.getObject("memtable"));
	}

	@Test
	@DisplayName("system_schema.columns encodes kind, position, clustering order and CQL type")
	void testColumns() {
		final var resultSet = select("columns");

		assertEquals(
			List.of("keyspace_name", "table_name", "column_name", "clustering_order",
				"column_name_bytes", "kind", "position", "type"), columnNames(resultSet));

		// Exactly what `SELECT * FROM system_schema.columns WHERE keyspace_name='d4'` returned on
		// cassandra:5.0.8, in the order it returned it - table_name ASC, then column_name ASC.
		assertEquals(List.of("awkward|ck1|asc|clustering|0|timeuuid",
				"awkward|ck2|desc|clustering|1|int", "awkward|home|none|regular|-1|frozen<address>",
				"awkward|items|none|regular|-1|list<double>", "awkward|pk1|none|partition_key|0|text",
				"awkward|pk2|none|partition_key|1|int",
				"awkward|places|none|regular|-1|map<text, frozen<address>>",
				"awkward|reg1|none|regular|-1|int", "awkward|scores|none|regular|-1|map<text, int>",
				"awkward|st1|none|static|-1|text", "awkward|tags|none|regular|-1|set<text>",
				"simple|id|none|partition_key|0|uuid", "simple|v|none|regular|-1|text"),
			rows(resultSet).stream()
				.map(row -> String.join("|", row.getString("table_name"), row.getString("column_name"),
					row.getString("clustering_order"), row.getString("kind"),
					String.valueOf(row.getInt("position")), row.getString("type")))
				.collect(toList()));
	}

	/**
	 * The driver never reads {@code column_name_bytes}, but a real node returns it, so the shape
	 * carries it. It is the column name's UTF-8 bytes.
	 */
	@Test
	@DisplayName("system_schema.columns carries column_name_bytes as the name's UTF-8 bytes")
	void testColumnNameBytes() {
		final var row = rows(select("columns")).get(0);

		assertEquals("ck1", row.getString("column_name"));
		assertArrayEqualsAsString("ck1", row);
	}

	@Test
	@DisplayName("system_schema.types projects a UDT's field names and CQL field types")
	void testTypes() {
		final var resultSet = select("types");

		assertEquals(List.of("keyspace_name", "type_name", "field_names", "field_types"),
			columnNames(resultSet));

		final var rows = rows(resultSet);
		assertEquals(1, rows.size());
		final var row = rows.get(0);
		assertEquals("d4", row.getString("keyspace_name"));
		assertEquals("address", row.getString("type_name"));
		assertEquals(List.of("street", "city", "zip"), row.getList("field_names", String.class));
		assertEquals(List.of("text", "text", "int"), row.getList("field_types", String.class));
	}

	@Test
	@DisplayName("system_schema.indexes projects the index kind and its target option")
	void testIndexes() {
		final var resultSet = select("indexes");

		assertEquals(List.of("keyspace_name", "table_name", "index_name", "kind", "options"),
			columnNames(resultSet));

		final var rows = rows(resultSet);
		assertEquals(1, rows.size());
		final var row = rows.get(0);
		assertEquals("d4", row.getString("keyspace_name"));
		assertEquals("awkward", row.getString("table_name"));
		assertEquals("awkward_reg1_idx", row.getString("index_name"));
		assertEquals("COMPOSITES", row.getString("kind"));
		assertEquals(Map.of("target", "reg1"), row.getMap("options", String.class, String.class));
	}

	/**
	 * A user-defined type is written unqualified. The driver's schema parser resolves it against the
	 * type map of the keyspace whose row it appears in, and a keyspace-qualified name - which
	 * {@code DataType#asCql} would produce - is an {@code IllegalStateException} in there.
	 */
	@Test
	@DisplayName("A UDT type string names the type alone, never keyspace-qualified")
	void testUserDefinedTypeIsNotQualified() {
		final var types = rows(select("columns")).stream()
			.map(row -> row.getString("type"))
			.filter(type -> type.contains("address"))
			.collect(toList());

		assertEquals(List.of("frozen<address>", "map<text, frozen<address>>"), types);
		assertTrue(types.stream().noneMatch(type -> type.contains("d4.")));
	}

	@Test
	@DisplayName("A frozen collection keeps the frozen keyword around the collection")
	void testFrozenCollectionTypeString() {
		session.execute("CREATE TABLE d4.frozen_collections (id int PRIMARY KEY, "
			+ "f frozen<list<text>>, g frozen<map<text, frozen<address>>>)");

		assertEquals(Map.of("f", "frozen<list<text>>", "g", "frozen<map<text, frozen<address>>>"),
			rows(select("columns")).stream()
				.filter(row -> "frozen_collections".equals(row.getString("table_name")))
				.filter(row -> !"id".equals(row.getString("column_name")))
				.collect(java.util.stream.Collectors.toMap(row -> row.getString("column_name"),
					row -> row.getString("type"))));
	}

	/**
	 * The end of the round trip: every type string this projects is handed back to the parser the
	 * driver actually uses on it, and has to come out as the type the model started with. That is
	 * what a connecting driver does with these rows, and it is stricter than comparing strings -
	 * frozenness and collection nesting have to survive too.
	 */
	@Test
	@DisplayName("Every projected type string parses back into the model's own DataType")
	void testTypeStringsRoundTripThroughTheDriversParser() {
		final var parser = new DataTypeCqlNameParser();
		final var keyspaceId = CqlIdentifier.fromInternal("d4");
		final var keyspace = context.getSeaStarKeyspace("d4")
			.orElseThrow(() -> new IllegalStateException("keyspace d4 is required by this test"));
		final Map<CqlIdentifier, UserDefinedType> userTypes = Map.copyOf(
			keyspace.getUserDefinedTypes());

		rows(select("columns")).forEach(row -> {
			final var table = keyspace.getSeaStarTable(row.getString("table_name"))
				.orElseThrow(() -> new IllegalStateException("every projected table exists"));
			final var expected = table.getColumns()
				.get(CqlIdentifier.fromInternal(row.getString("column_name")))
				.getType();

			assertEquals(expected, parser.parse(keyspaceId, row.getString("type"), userTypes,
				(InternalDriverContext) context), row.getString("type"));
		});

		final var fieldTypes = rows(select("types")).get(0).getList("field_types", String.class);
		assertEquals(keyspace.getSeaStarUserDefinedType("address")
			.orElseThrow(() -> new IllegalStateException("type address is required by this test"))
			.getFieldTypes(), fieldTypes.stream()
			.map(type -> parser.parse(keyspaceId, type, userTypes, (InternalDriverContext) context))
			.collect(toList()));
	}

	@Test
	@DisplayName("views, functions and aggregates are always empty but still describe their columns")
	void testUnsupportedTablesAreEmpty() {
		assertTrue(rows(select("views")).isEmpty());
		assertTrue(rows(select("functions")).isEmpty());
		assertTrue(rows(select("aggregates")).isEmpty());

		assertEquals(
			List.of("keyspace_name", "view_name", "additional_write_policy", "allow_auto_snapshot",
				"base_table_id", "base_table_name", "bloom_filter_fp_chance", "caching", "cdc",
				"comment", "compaction", "compression", "crc_check_chance",
				"dclocal_read_repair_chance", "default_time_to_live", "extensions",
				"gc_grace_seconds", "id", "include_all_columns", "incremental_backups",
				"max_index_interval", "memtable", "memtable_flush_period_in_ms", "min_index_interval",
				"read_repair", "read_repair_chance", "speculative_retry", "where_clause"),
			columnNames(select("views")));
		assertEquals(List.of("keyspace_name", "function_name", "argument_types", "argument_names",
			"body", "called_on_null_input", "language", "return_type"), columnNames(
			select("functions")));
		assertEquals(List.of("keyspace_name", "aggregate_name", "argument_types", "final_func",
			"initcond", "return_type", "state_func", "state_type"), columnNames(select("aggregates")));
	}

	@Test
	@DisplayName("A table system_schema does not have projects nothing")
	void testUnknownTable() {
		assertTrue(SystemSchema.select(context, "peers").isEmpty());
		assertTrue(SystemSchema.select(context, "").isEmpty());
	}

	@Test
	@DisplayName("The projected column definitions name the system_schema keyspace and table")
	void testColumnDefinitionsAreQualified() {
		final var definitions = select("columns").getColumnDefinitions();

		assertEquals(CqlIdentifier.fromInternal("system_schema"), definitions.get(0).getKeyspace());
		assertEquals(CqlIdentifier.fromInternal("columns"), definitions.get(0).getTable());
		assertEquals(DataTypes.TEXT, definitions.get("keyspace_name").getType());
		assertEquals(DataTypes.INT, definitions.get("position").getType());
		assertEquals(DataTypes.BLOB, definitions.get("column_name_bytes").getType());
	}

	@Test
	@DisplayName("The projection is live: a table created after a call shows up in the next one")
	void testProjectionIsLive() {
		assertEquals(2, rows(select("tables")).size());

		session.execute("CREATE TABLE d4.later (id int PRIMARY KEY)");

		assertEquals(List.of("awkward", "later", "simple"),
			rows(select("tables")).stream().map(row -> row.getString("table_name")).collect(toList()));
	}

	/**
	 * d_plan D6. The projection is a projection, not a keyspace: an in-process user who never starts
	 * a server must not suddenly find invented system keyspaces in their metadata. If this fails,
	 * someone took the real-keyspace route - which is a deliberate, documented change to the core,
	 * not something to land by accident.
	 */
	@Test
	@DisplayName("The projection does not add system keyspaces to the session's metadata")
	void testSystemKeyspacesDoNotLeakIntoMetadata() {
		select("keyspaces");
		select("tables");
		select("columns");

		final var keyspaces = session.getMetadata().getKeyspaces();
		assertFalse(keyspaces.containsKey(CqlIdentifier.fromInternal("system_schema")));
		assertFalse(keyspaces.containsKey(CqlIdentifier.fromInternal("system")));
		assertEquals(Set.of(CqlIdentifier.fromInternal("d4")), keyspaces.keySet());
		assertTrue(context.getSeaStarKeyspace("system_schema").isEmpty());
	}

	private AsyncResultSet select(final String table) {
		return SystemSchema.select(context, table)
			.orElseThrow(() -> new IllegalStateException("system_schema." + table + " is required"));
	}

	private static List<String> columnNames(final AsyncResultSet resultSet) {
		return StreamSupport.stream(resultSet.getColumnDefinitions().spliterator(), false)
			.map(definition -> definition.getName().asInternal())
			.collect(toList());
	}

	private static List<Row> rows(final AsyncResultSet resultSet) {
		return StreamSupport.stream(resultSet.currentPage().spliterator(), false).collect(toList());
	}

	private static void assertArrayEqualsAsString(final String expected, final Row row) {
		final var bytes = row.getByteBuffer("column_name_bytes");
		assertNotNull(bytes);
		final var actual = new byte[bytes.remaining()];
		bytes.duplicate().get(actual);

		assertEquals(expected, new String(actual, StandardCharsets.UTF_8));
	}

}
