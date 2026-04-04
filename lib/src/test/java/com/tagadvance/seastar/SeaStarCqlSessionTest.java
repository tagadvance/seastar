package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.List;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.junit.jupiter.api.Test;

class SeaStarCqlSessionTest {

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

	@Test
	void testSimpleSelect() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			final var context = session.getContext();
			final var keyspace = context.node.newSeaStarKeyspace(CqlIdentifier.fromInternal("foo"));
			final var table = keyspace.newSeaStarTable(CqlIdentifier.fromInternal("bar"),
				List.of());

			final var statement = session.prepare("SELECT * FROM foo.bar").bind();
			final var resultSet = session.execute(statement);
			System.out.println(resultSet);
		}
	}

}
