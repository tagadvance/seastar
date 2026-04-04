package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.tagadvance.seastar.VolatileTable.VolatileColumn;
import java.util.List;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.junit.jupiter.api.Test;

class SeaStarCqlSessionTest {

	@Test
	void testSimpleSelect() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			final var keyspace = context.node.newSeaStarKeyspace(CqlIdentifier.fromInternal("foo"));
			final var tableName = CqlIdentifier.fromInternal("bar");
			// TODO: TABLE BUILDER / populate from query
			final var table = keyspace.newSeaStarTable(tableName, List.of(
				new VolatileColumn(keyspace.name(), tableName, CqlIdentifier.fromInternal("foo"),
					DataTypes.TEXT, false, false),
				new VolatileColumn(keyspace.name(), tableName, CqlIdentifier.fromInternal("bar"),
					DataTypes.TEXT, false, false)));
			table.addRow("foo", "bar");

			final var resultSet = session.execute("SELECT * FROM foo.bar");
			assertNotNull(resultSet);
			assertTrue(resultSet.wasApplied());
			var all = resultSet.all();
			assertEquals(1, all.size());
			final Row row = all.get(0);
			assertEquals("foo", row.getString(0));
			assertEquals("foo", row.getString("foo"));
			assertEquals("foo", row.getString(CqlIdentifier.fromInternal("foo")));
			assertEquals("bar", row.getString(1));
			assertEquals("bar", row.getString("bar"));
			assertEquals("bar", row.getString(CqlIdentifier.fromInternal("bar")));
		}
	}

	@Test
	void testCreateKeyspace() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			assertTrue(
				context.node.getSeaStarKeyspace(CqlIdentifier.fromInternal("foo")).isEmpty());

			final var resultSet1 = session.execute(
				"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
			assertNotNull(resultSet1);

			final var resultSet2 = assertDoesNotThrow(() -> session.execute(
				"CREATE KEYSPACE IF NOT EXISTS foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));
			assertNotNull(resultSet2);

			assertThrows(AlreadyExistsException.class, () -> session.execute(
				"CREATE KEYSPACE foo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));

			assertTrue(
				context.node.getSeaStarKeyspace(CqlIdentifier.fromInternal("foo")).isPresent());
		}
	}


}
