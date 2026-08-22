package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * Lightweight transactions - {@code IF NOT EXISTS}, {@code IF EXISTS} and {@code IF} conditions,
 * with the {@code [applied]} answer each returns - and batches. This group owns the {@code lwt}
 * keyspace.
 */
public abstract class AbstractLwtFidelityTest extends AbstractFidelityTest {

	@Override
	protected void initialize() {
		createKeyspace("lwt");
	}

	private void createLwtTable() {
		session.execute(
			"CREATE TABLE IF NOT EXISTS lwt.lwt (id int PRIMARY KEY, name text, age int)");
	}

	private String lwtName(final int id) {
		final var row = session.execute("SELECT name FROM lwt.lwt WHERE id = " + id).one();

		return row == null ? null : row.getString("name");
	}

	@Test
	@Order(36)
	@DisplayName("INSERT IF NOT EXISTS returns [applied]=true and stores the row")
	void testInsertIfNotExistsApplied() {
		createLwtTable();

		final var resultSet = session.execute(
			"INSERT INTO lwt.lwt (id, name, age) VALUES (36, 'Ada', 30) IF NOT EXISTS");

		assertTrue(resultSet.wasApplied());
		assertEquals("Ada", lwtName(36));
	}

	@Test
	@Order(37)
	@DisplayName("INSERT IF NOT EXISTS on an existing key returns [applied]=false with the current row")
	void testInsertIfNotExistsRejected() {
		createLwtTable();
		session.execute("INSERT INTO lwt.lwt (id, name, age) VALUES (37, 'Grace', 40) IF NOT EXISTS");

		final var resultSet = session.execute(
			"INSERT INTO lwt.lwt (id, name, age) VALUES (37, 'Hopper', 41) IF NOT EXISTS");

		assertFalse(resultSet.wasApplied());
		final var row = resultSet.one();
		assertEquals("Grace", row.getString("name"));
		assertEquals(40, row.getInt("age"));
		assertEquals("Grace", lwtName(37));
	}

	@Test
	@Order(38)
	@DisplayName("UPDATE IF <condition> applies when the condition holds")
	void testUpdateIfConditionApplied() {
		createLwtTable();
		session.execute("INSERT INTO lwt.lwt (id, name, age) VALUES (38, 'old', 1)");

		final var resultSet = session.execute(
			"UPDATE lwt.lwt SET name = 'new' WHERE id = 38 IF name = 'old'");

		assertTrue(resultSet.wasApplied());
		assertEquals("new", lwtName(38));
	}

	@Test
	@Order(39)
	@DisplayName("UPDATE IF <condition> is rejected and reports current values when it fails")
	void testUpdateIfConditionRejected() {
		createLwtTable();
		session.execute("INSERT INTO lwt.lwt (id, name, age) VALUES (39, 'keep', 1)");

		final var resultSet = session.execute(
			"UPDATE lwt.lwt SET name = 'changed' WHERE id = 39 IF name = 'wrong'");

		assertFalse(resultSet.wasApplied());
		assertEquals("keep", resultSet.one().getString("name"));
		assertEquals("keep", lwtName(39));
	}

	@Test
	@Order(40)
	@DisplayName("UPDATE IF EXISTS applies only when the row exists")
	void testUpdateIfExists() {
		createLwtTable();

		final var missing = session.execute(
			"UPDATE lwt.lwt SET name = 'ghost' WHERE id = 4000 IF EXISTS");
		assertFalse(missing.wasApplied());
		assertNull(lwtName(4000));

		session.execute("INSERT INTO lwt.lwt (id, name, age) VALUES (40, 'here', 1)");
		final var present = session.execute(
			"UPDATE lwt.lwt SET name = 'updated' WHERE id = 40 IF EXISTS");
		assertTrue(present.wasApplied());
		assertEquals("updated", lwtName(40));
	}

	@Test
	@Order(41)
	@DisplayName("DELETE IF EXISTS applies only when the row exists")
	void testDeleteIfExists() {
		createLwtTable();

		final var missing = session.execute("DELETE FROM lwt.lwt WHERE id = 4100 IF EXISTS");
		assertFalse(missing.wasApplied());

		session.execute("INSERT INTO lwt.lwt (id, name, age) VALUES (41, 'doomed', 1)");
		final var present = session.execute("DELETE FROM lwt.lwt WHERE id = 41 IF EXISTS");
		assertTrue(present.wasApplied());
		assertNull(lwtName(41));
	}

	@Test
	@Order(42)
	@DisplayName("DELETE IF <condition> deletes only when the condition holds")
	void testDeleteIfCondition() {
		createLwtTable();
		session.execute("INSERT INTO lwt.lwt (id, name, age) VALUES (42, 'delete-me', 1)");

		final var rejected = session.execute(
			"DELETE FROM lwt.lwt WHERE id = 42 IF name = 'nope'");
		assertFalse(rejected.wasApplied());
		assertEquals("delete-me", lwtName(42));

		final var applied = session.execute(
			"DELETE FROM lwt.lwt WHERE id = 42 IF name = 'delete-me'");
		assertTrue(applied.wasApplied());
		assertNull(lwtName(42));
	}

	@Test
	@Order(43)
	@DisplayName("Conditional UPDATE on an undefined column throws InvalidQueryException")
	void testConditionalUpdateUndefinedColumn() {
		createLwtTable();

		assertThrows(InvalidQueryException.class, () -> session.execute(
			"UPDATE lwt.lwt SET name = 'x' WHERE id = 43 IF nope = 1"));
	}

	@Test
	@Order(44)
	@DisplayName("INSERT upsert merges named columns and preserves unspecified ones")
	void testInsertUpsertMergesColumns() {
		createLwtTable();
		session.execute("INSERT INTO lwt.lwt (id, name, age) VALUES (44, 'orig', 10)");

		session.execute("INSERT INTO lwt.lwt (id, age) VALUES (44, 99)");

		final var row = session.execute("SELECT name, age FROM lwt.lwt WHERE id = 44").one();
		assertEquals("orig", row.getString("name"));
		assertEquals(99, row.getInt("age"));
	}

	@Test
	@Order(45)
	@DisplayName("BATCH parsed from a CQL string applies every child statement")
	void testCqlStringBatch() {
		createLwtTable();

		session.execute("BEGIN BATCH "
			+ "INSERT INTO lwt.lwt (id, name, age) VALUES (45, 'batch-a', 1); "
			+ "UPDATE lwt.lwt SET name = 'batch-b' WHERE id = 45; "
			+ "APPLY BATCH");

		assertEquals("batch-b", lwtName(45));
	}

	@Test
	@Order(46)
	@DisplayName("Driver BatchStatement applies every child statement")
	void testDriverBatchStatement() {
		createLwtTable();

		final var batch = BatchStatement.builder(DefaultBatchType.LOGGED)
			.addStatement(
				SimpleStatement.newInstance("INSERT INTO lwt.lwt (id, name) VALUES (46, 'driver-a')"))
			.addStatement(
				SimpleStatement.newInstance("UPDATE lwt.lwt SET name = 'driver-b' WHERE id = 46"))
			.build();

		final var result = session.execute(batch);

		assertTrue(result.wasApplied());
		assertEquals("driver-b", lwtName(46));
	}

	@Test
	@Order(47)
	@DisplayName("A SELECT inside a batch is rejected with InvalidQueryException")
	void testSelectInBatchRejected() {
		createLwtTable();

		final var batch = BatchStatement.builder(DefaultBatchType.LOGGED)
			.addStatement(SimpleStatement.newInstance("SELECT * FROM lwt.lwt WHERE id = 47"))
			.build();

		assertThrows(InvalidQueryException.class, () -> session.execute(batch));
	}

	@Test
	@Order(247)
	@DisplayName("A failed LWT's result row answers allIndicesOf for [applied]")
	void testAppliedRowAllIndicesOf() {
		createLwtTable();
		session.execute("INSERT INTO lwt.lwt (id, name, age) VALUES (247, 'first', 1)");

		final var row = session.execute(
			"INSERT INTO lwt.lwt (id, name, age) VALUES (247, 'second', 2) IF NOT EXISTS").one();
		assertFalse(row.getBoolean("[applied]"));
		assertEquals(List.of(0), row.allIndicesOf("[applied]"));
		assertThrows(IllegalArgumentException.class, () -> row.allIndicesOf("nope"));
	}

}
