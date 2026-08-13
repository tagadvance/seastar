package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * The schema statements: keyspaces, tables and secondary indexes - created, altered, truncated and
 * dropped. This group owns the {@code ddl}, {@code throwaway}, {@code repl_default},
 * {@code repl_durable_off} and {@code alter_ks} keyspaces.
 */
public abstract class AbstractSchemaFidelityTest extends AbstractFidelityTest {

	@Test
	@Order(1)
	void testCreateKeyspace() {
		final var context = session.getContext();
		if (context instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(
				seaStarContext.getSeaStarKeyspace(CqlIdentifier.fromInternal("ddl")).isEmpty());
		}

		final var resultSet1 = session.execute(
			"CREATE KEYSPACE ddl WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		assertNotNull(resultSet1);

		final var resultSet2 = assertDoesNotThrow(() -> session.execute(
			"CREATE KEYSPACE IF NOT EXISTS ddl WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));
		assertNotNull(resultSet2);

		assertThrows(AlreadyExistsException.class, () -> session.execute(
			"CREATE KEYSPACE ddl WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));

		if (context instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(
				seaStarContext.getSeaStarKeyspace(CqlIdentifier.fromInternal("ddl")).isPresent());
		}
	}

	@Test
	@Order(3)
	void testCreateSimpleTable() {
		final var context = session.getContext();

		final var resultSet1 = session.execute("""
			CREATE TABLE ddl.users (
			    user_id UUID PRIMARY KEY,
			    first_name text
			);""");
		assertNotNull(resultSet1);

		final var resultSet2 = assertDoesNotThrow(() -> session.execute("""
			CREATE TABLE IF NOT EXISTS ddl.users (
			    user_id UUID PRIMARY KEY,
			    first_name text
			);"""));
		assertNotNull(resultSet2);

		assertThrows(AlreadyExistsException.class, () -> session.execute("""
			CREATE TABLE ddl.users (
			    user_id UUID PRIMARY KEY,
			    first_name text
			);"""));

		if (context instanceof SeaStarDriverContext seaStarContext) {
			final var table = seaStarContext.getSeaStarKeyspace("ddl")
				.flatMap(keyspace -> keyspace.getSeaStarTable("users"));
			assertTrue(table.isPresent());

			final var columns = table.get().getColumns();
			assertEquals(DataTypes.UUID,
				columns.get(CqlIdentifier.fromInternal("user_id")).getType());
			assertEquals(DataTypes.TEXT,
				columns.get(CqlIdentifier.fromInternal("first_name")).getType());

			assertEquals(List.of(CqlIdentifier.fromInternal("user_id")),
				table.get().getPartitionKey().stream().map(ColumnMetadata::getName).toList());
			assertTrue(table.get().getClusteringColumns().isEmpty());
		}
	}

	@Test
	@Order(5)
	void testCreateTableWithPrimaryKeyAndClusteringColumn() {
		final var context = session.getContext();

		final var resultSet1 = session.execute("""
			CREATE TABLE ddl.user_status_updates (
				user_id uuid,
				updated_at timestamp,
				status text,
				PRIMARY KEY (user_id, updated_at)
			);""");
		assertNotNull(resultSet1);

		if (context instanceof SeaStarDriverContext seaStarContext) {
			final var table = seaStarContext.getSeaStarKeyspace("ddl")
				.flatMap(keyspace -> keyspace.getSeaStarTable("user_status_updates"));
			assertTrue(table.isPresent());

			assertEquals(List.of(CqlIdentifier.fromInternal("user_id")),
				table.get().getPartitionKey().stream().map(ColumnMetadata::getName).toList());

			final var clustering = table.get().getClusteringColumns();
			assertEquals(List.of(CqlIdentifier.fromInternal("updated_at")),
				clustering.keySet().stream().map(ColumnMetadata::getName).toList());
			assertEquals(ClusteringOrder.ASC,
				clustering.values().iterator().next());
		}
	}

	@Test
	@Order(6)
	void testCreateTableWithPrimaryKeysAndClusteringColumn() {
		final var context = session.getContext();

		final var resultSet1 = session.execute("""
			CREATE TABLE ddl.device_metrics (
				device_id uuid,
				log_date date,
				log_time time,
				metric_value double,
				PRIMARY KEY ((device_id, log_date), log_time)
			);""");
		assertNotNull(resultSet1);

		if (context instanceof SeaStarDriverContext seaStarContext) {
			final var table = seaStarContext.getSeaStarKeyspace("ddl")
				.flatMap(keyspace -> keyspace.getSeaStarTable("device_metrics"));
			assertTrue(table.isPresent());

			assertEquals(
				List.of(CqlIdentifier.fromInternal("device_id"),
					CqlIdentifier.fromInternal("log_date")),
				table.get().getPartitionKey().stream().map(ColumnMetadata::getName).toList());
			assertEquals(List.of(CqlIdentifier.fromInternal("log_time")),
				table.get().getClusteringColumns().keySet().stream().map(ColumnMetadata::getName)
					.toList());
		}
	}

	@Test
	@Order(28)
	@DisplayName("DROP TABLE removes the table; dropping it again throws unless IF EXISTS")
	void testDropTable() {
		session.execute(
			"CREATE TABLE ddl.doomed (id uuid PRIMARY KEY, name text)");

		session.execute("DROP TABLE ddl.doomed");

		if (session.getContext() instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(seaStarContext.getSeaStarKeyspace("ddl")
				.flatMap(keyspace -> keyspace.getSeaStarTable("doomed")).isEmpty());
		}

		assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP TABLE ddl.doomed"));

		assertDoesNotThrow(() -> session.execute("DROP TABLE IF EXISTS ddl.doomed"));
	}

	@Test
	@Order(29)
	@DisplayName("DROP KEYSPACE removes the keyspace; dropping it again throws unless IF EXISTS")
	void testDropKeyspace() {
		session.execute(
			"CREATE KEYSPACE throwaway WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

		session.execute("DROP KEYSPACE throwaway");

		if (session.getContext() instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(seaStarContext.getSeaStarKeyspace("throwaway").isEmpty());
		}

		assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP KEYSPACE throwaway"));

		assertDoesNotThrow(() -> session.execute("DROP KEYSPACE IF EXISTS throwaway"));
	}

	@Test
	@Order(30)
	@DisplayName("TRUNCATE empties the table but keeps it; unknown table throws InvalidQueryException")
	void testTruncate() {
		session.execute("CREATE TABLE ddl.temporary (id uuid PRIMARY KEY, name text)");
		session.execute(
			"INSERT INTO ddl.temporary (id, name) VALUES (523e4567-e89b-12d3-a456-426614174004, 'Eve')");

		session.execute("TRUNCATE ddl.temporary");

		assertEquals(0, session.execute("SELECT * FROM ddl.temporary").all().size());

		if (session.getContext() instanceof SeaStarDriverContext seaStarContext) {
			assertTrue(seaStarContext.getSeaStarKeyspace("ddl")
				.flatMap(keyspace -> keyspace.getSeaStarTable("temporary")).isPresent());
		}

		assertThrows(InvalidQueryException.class,
			() -> session.execute("TRUNCATE TABLE ddl.nope"));
	}

	private void createIndexedTable() {
		session.execute("CREATE TABLE IF NOT EXISTS ddl.indexed (id int PRIMARY KEY, name text)");
		session.execute("CREATE INDEX IF NOT EXISTS ON ddl.indexed (name)");
	}

	@Test
	@Order(50)
	@DisplayName("CREATE INDEX exposes the index through TableMetadata.getIndexes")
	void testCreateIndexExposedInMetadata() {
		createIndexedTable();

		final var indexes = session.getMetadata().getKeyspace("ddl")
			.flatMap(keyspace -> keyspace.getTable("indexed"))
			.map(table -> table.getIndexes())
			.orElseThrow();
		assertTrue(indexes.containsKey(CqlIdentifier.fromInternal("indexed_name_idx")));
	}

	@Test
	@Order(51)
	@DisplayName("An indexed column can be queried without ALLOW FILTERING")
	void testQueryIndexedColumnWithoutFiltering() {
		createIndexedTable();
		session.execute("INSERT INTO ddl.indexed (id, name) VALUES (1, 'indexed-a')");
		session.execute("INSERT INTO ddl.indexed (id, name) VALUES (2, 'indexed-b')");

		final var rows = session.execute("SELECT id FROM ddl.indexed WHERE name = 'indexed-a'").all();

		assertEquals(1, rows.size());
		assertEquals(1, rows.get(0).getInt("id"));
	}

	@Test
	@Order(52)
	@DisplayName("Indexing an undefined column throws InvalidQueryException")
	void testCreateIndexOnUndefinedColumn() {
		createIndexedTable();

		assertThrows(InvalidQueryException.class,
			() -> session.execute("CREATE INDEX ON ddl.indexed (nope)"));
	}

	@Test
	@Order(53)
	@DisplayName("Creating a duplicate index throws unless IF NOT EXISTS")
	void testCreateDuplicateIndex() {
		createIndexedTable();

		assertThrows(InvalidQueryException.class,
			() -> session.execute("CREATE INDEX indexed_name_idx ON ddl.indexed (name)"));
		assertDoesNotThrow(
			() -> session.execute("CREATE INDEX IF NOT EXISTS indexed_name_idx ON ddl.indexed (name)"));
	}

	@Test
	@Order(86)
	@DisplayName("Keyspace metadata reports the replication and durable writes it was created with")
	void testKeyspaceReplicationMetadata() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS repl_default WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		session.execute("CREATE KEYSPACE IF NOT EXISTS repl_durable_off WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 } AND durable_writes = false");

		final var defaults = keyspaceMetadata("repl_default");
		assertEquals(
			Map.of("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor",
				"1"), defaults.getReplication());
		assertTrue(defaults.isDurableWrites());

		final var durableOff = keyspaceMetadata("repl_durable_off");
		assertEquals("2", durableOff.getReplication().get("replication_factor"));
		assertFalse(durableOff.isDurableWrites());
	}

	private KeyspaceMetadata keyspaceMetadata(final String name) {
		return session.getMetadata()
			.getKeyspace(name)
			.orElseThrow(() -> new IllegalStateException(
				"keyspace %s is required to read its metadata".formatted(name)));
	}

	@Test
	@Order(140)
	@DisplayName("ALTER TABLE ADD gives every existing row a null in the new column")
	void testAlterTableAdd() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS ddl.alter_add (pk int PRIMARY KEY, z text, m text)");
		session.execute("INSERT INTO ddl.alter_add (pk, z, m) VALUES (1, 'zz', 'mm')");

		session.execute("ALTER TABLE ddl.alter_add ADD a text");

		// A new column lands after the primary key, in alphabetical order among the rest.
		assertEquals(List.of("pk", "a", "m", "z"), columnNames("SELECT * FROM ddl.alter_add"));
		final var row = session.execute("SELECT * FROM ddl.alter_add WHERE pk = 1").one();
		assertNotNull(row);
		assertNull(row.getString("a"));
		assertEquals("zz", row.getString("z"));

		session.execute("ALTER TABLE ddl.alter_add ADD (b text, c int)");
		assertDoesNotThrow(
			() -> session.execute("ALTER TABLE ddl.alter_add ADD IF NOT EXISTS b text"));
		assertEquals(List.of("pk", "a", "b", "c", "m", "z"),
			columnNames("SELECT * FROM ddl.alter_add"));
	}

	@Test
	@Order(141)
	@DisplayName("ALTER TABLE DROP discards the column and the values it held")
	void testAlterTableDrop() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS ddl.alter_drop (pk int PRIMARY KEY, a text, b text)");
		session.execute("INSERT INTO ddl.alter_drop (pk, a, b) VALUES (1, 'aa', 'bb')");

		session.execute("ALTER TABLE ddl.alter_drop DROP a");

		assertEquals(List.of("pk", "b"), columnNames("SELECT * FROM ddl.alter_drop"));
		assertEquals("bb", session.execute("SELECT b FROM ddl.alter_drop WHERE pk = 1")
			.one()
			.getString("b"));

		// Re-adding the column brings it back empty; the dropped values do not come back.
		session.execute("ALTER TABLE ddl.alter_drop ADD a text");
		assertNull(session.execute("SELECT a FROM ddl.alter_drop WHERE pk = 1").one().getString("a"));
		assertDoesNotThrow(() -> session.execute("ALTER TABLE ddl.alter_drop DROP IF EXISTS nope"));
	}

	@Test
	@Order(142)
	@DisplayName("ALTER TABLE RENAME renames a primary key column and keeps its data")
	void testAlterTableRename() {
		session.execute("CREATE TABLE IF NOT EXISTS ddl.alter_rename (pk int, ck int, v text, "
			+ "PRIMARY KEY (pk, ck))");
		session.execute("INSERT INTO ddl.alter_rename (pk, ck, v) VALUES (1, 2, 'x')");

		session.execute("ALTER TABLE ddl.alter_rename RENAME ck TO ck2");

		assertEquals(List.of("pk", "ck2", "v"), columnNames("SELECT * FROM ddl.alter_rename"));
		final var row = session.execute(
			"SELECT * FROM ddl.alter_rename WHERE pk = 1 AND ck2 = 2").one();
		assertNotNull(row);
		assertEquals("x", row.getString("v"));
	}

	@Test
	@Order(143)
	@DisplayName("ALTER TABLE reports the column or table at fault, and IF EXISTS forgives absence")
	void testAlterTableIsRejected() {
		session.execute("CREATE TABLE IF NOT EXISTS ddl.alter_bad (pk int, ck int, v text, "
			+ "PRIMARY KEY (pk, ck))");

		assertMentions("v", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TABLE ddl.alter_bad ADD v text")));
		assertMentions("nope", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TABLE ddl.alter_bad DROP nope")));
		// A key column can be neither dropped nor, once it is not one, renamed.
		assertMentions("pk", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TABLE ddl.alter_bad DROP pk")));
		assertMentions("v", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TABLE ddl.alter_bad RENAME v TO v2")));
		assertMentions("nope", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TABLE ddl.alter_bad RENAME nope TO nope2")));
		assertMentions("already exists", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TABLE ddl.alter_bad RENAME pk TO ck")));
		// Cassandra 5 dropped support for changing a column's type outright.
		assertMentions("no longer supported", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TABLE ddl.alter_bad ALTER v TYPE blob")));
		assertMentions("alter_missing", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER TABLE ddl.alter_missing ADD x int")));

		// IF EXISTS forgives a missing table, and a missing column on a DROP.
		assertDoesNotThrow(
			() -> session.execute("ALTER TABLE IF EXISTS ddl.alter_missing ADD x int"));
		assertDoesNotThrow(
			() -> session.execute("ALTER TABLE ddl.alter_bad DROP IF EXISTS nope"));
		// Table options are not modelled, but the statement is still accepted.
		assertDoesNotThrow(
			() -> session.execute("ALTER TABLE ddl.alter_bad WITH comment = 'hello'"));
	}

	@Test
	@Order(144)
	@DisplayName("DROP INDEX removes the index, and reports one that is not there by name")
	void testDropIndex() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS ddl.drop_index (id int PRIMARY KEY, name text)");
		session.execute("CREATE INDEX IF NOT EXISTS drop_index_name_idx ON ddl.drop_index (name)");
		assertTrue(indexNames("drop_index").contains("drop_index_name_idx"));

		session.execute("DROP INDEX ddl.drop_index_name_idx");

		assertFalse(indexNames("drop_index").contains("drop_index_name_idx"));
		// An index that is not there is named in the failure, and forgiven by IF EXISTS.
		assertMentions("drop_index_name_idx", assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP INDEX ddl.drop_index_name_idx")));
		assertDoesNotThrow(() -> session.execute("DROP INDEX IF EXISTS ddl.drop_index_name_idx"));
		// A missing keyspace reads as a missing index, because the index is what was looked up.
		assertMentions("nope", assertThrows(InvalidQueryException.class,
			() -> session.execute("DROP INDEX nope.nope_idx")));
		assertDoesNotThrow(() -> session.execute("DROP INDEX IF EXISTS nope.nope_idx"));
	}

	private Set<String> indexNames(final String table) {
		return session.getMetadata()
			.getKeyspace("ddl")
			.flatMap(keyspace -> keyspace.getTable(table))
			.orElseThrow()
			.getIndexes()
			.keySet()
			.stream()
			.map(CqlIdentifier::asInternal)
			.collect(Collectors.toSet());
	}

	@Test
	@Order(146)
	@DisplayName("ALTER KEYSPACE replaces the options it names and leaves the rest alone")
	void testAlterKeyspace() {
		final var replicated = Map.of("class", "org.apache.cassandra.locator.SimpleStrategy",
			"replication_factor", "2");
		session.execute("CREATE KEYSPACE IF NOT EXISTS alter_ks WITH replication = "
			+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");

		session.execute("ALTER KEYSPACE alter_ks WITH replication = "
			+ "{'class': 'SimpleStrategy', 'replication_factor': 2}");

		assertEquals(replicated, keyspaceMetadata("alter_ks").getReplication());
		assertTrue(keyspaceMetadata("alter_ks").isDurableWrites());

		// Naming only durable_writes leaves the replication as it was.
		session.execute("ALTER KEYSPACE alter_ks WITH durable_writes = false");

		assertFalse(keyspaceMetadata("alter_ks").isDurableWrites());
		assertEquals(replicated, keyspaceMetadata("alter_ks").getReplication());

		assertMentions("nope", assertThrows(InvalidQueryException.class,
			() -> session.execute("ALTER KEYSPACE nope WITH durable_writes = false")));
		assertDoesNotThrow(() -> session.execute(
			"ALTER KEYSPACE IF EXISTS nope WITH durable_writes = false"));
	}

}
