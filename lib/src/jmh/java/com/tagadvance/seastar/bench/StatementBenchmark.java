package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Warm per-statement cost. Every benchmark runs against a table seeded with
 * {@value #ROWS} rows, because SeaStar has no index on the partition key and every statement with a
 * WHERE clause scans the table - the row count is part of the measurement, not an accident of it.
 *
 * <p>JMH gives each {@code @Benchmark} method its own state instance, so the mutating benchmarks do
 * not contaminate each other.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class StatementBenchmark {

	static final int ROWS = 1_000;

	private static final String TABLE = "people";

	private static final int EXISTING_KEY = ROWS / 2;

	private CqlSession session;
	private PreparedStatement insert;
	private PreparedStatement update;
	private PreparedStatement delete;
	private PreparedStatement pointSelect;
	private BoundStatement boundInsert;
	private BatchStatement batch;
	private SimpleStatement scan;
	private long ddlCounter;
	private long uncachedCounter;

	@Setup(Level.Trial)
	public void setUp() {
		session = BenchmarkFixture.newSession();
		BenchmarkFixture.seed(session, TABLE, ROWS);
		session.execute("CREATE TABLE %s (id int PRIMARY KEY, name text)"
			.formatted(BenchmarkFixture.qualify("batched")));

		final var qualified = BenchmarkFixture.qualify(TABLE);
		insert = session.prepare("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)".formatted(qualified));
		update = session.prepare("UPDATE %s SET name = ? WHERE id = ?".formatted(qualified));
		delete = session.prepare("DELETE FROM %s WHERE id = ?".formatted(qualified));
		pointSelect = session.prepare("SELECT name FROM %s WHERE id = ?".formatted(qualified));
		boundInsert = insert.bind(EXISTING_KEY, "bound", 1);
		scan = SimpleStatement.newInstance("SELECT * FROM " + qualified);

		final var builder = BatchStatement.builder(DefaultBatchType.LOGGED);
		IntStream.range(0, 100)
			.mapToObj(id -> SimpleStatement.newInstance(
				"INSERT INTO %s (id, name) VALUES (%d, 'batched')".formatted(
					BenchmarkFixture.qualify("batched"), id)))
			.forEach(builder::addStatement);
		batch = builder.build();
	}

	/**
	 * CREATE TABLE has to name a new table every time, so the DDL keyspace is dropped and recreated
	 * per iteration rather than accumulating tables for the length of the run.
	 */
	@Setup(Level.Iteration)
	public void resetDdlKeyspace() {
		session.execute("DROP KEYSPACE IF EXISTS bench_ddl");
		session.execute(
			"CREATE KEYSPACE bench_ddl WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
		ddlCounter = 0;
	}

	@TearDown(Level.Trial)
	public void tearDown() {
		session.close();
	}

	@Benchmark
	public Object createTable() {
		return session.execute(
			"CREATE TABLE bench_ddl.t_%d (id uuid PRIMARY KEY, name text, age int)".formatted(
				ddlCounter++));
	}

	@Benchmark
	public Object insertLiteral() {
		return session.execute(
			"INSERT INTO %s (id, name, age) VALUES (%d, 'literal', 1)".formatted(
				BenchmarkFixture.qualify(TABLE), EXISTING_KEY));
	}

	@Benchmark
	public Object insertPrepared() {
		return session.execute(boundInsert);
	}

	@Benchmark
	public Row selectPoint() {
		return session.execute(pointSelect.bind(EXISTING_KEY)).one();
	}

	@Benchmark
	public List<Row> selectScan() {
		return session.execute(scan).all();
	}

	@Benchmark
	public Object update() {
		return session.execute(update.bind("updated", EXISTING_KEY));
	}

	/**
	 * The row is removed by the first invocation, so steady state measures parse, resolution and the
	 * full-table scan that finds nothing - which is where the time goes either way.
	 */
	@Benchmark
	public Object deleteByPrimaryKey() {
		return session.execute(delete.bind(EXISTING_KEY));
	}

	@Benchmark
	public ResultSet batch100() {
		return session.execute(batch);
	}

	@Benchmark
	public PreparedStatement prepareCached() {
		return session.prepare(
			"SELECT name FROM %s WHERE id = ?".formatted(BenchmarkFixture.qualify(TABLE)));
	}

	/**
	 * A distinct query string per invocation, so the prepared statement cache always misses.
	 */
	@Benchmark
	public PreparedStatement prepareUncached() {
		return session.prepare(uniqueQuery());
	}

	/**
	 * SeaStar's {@code prepare} is lazy - it parses on first access to the bind variable metadata,
	 * not in {@code prepare} itself - so the two benchmarks above measure only the cache lookup and
	 * the statement object. This one forces the parse that a caller pays on the first bind.
	 */
	@Benchmark
	public ColumnDefinitions prepareUncachedResolved() {
		return session.prepare(uniqueQuery()).getVariableDefinitions();
	}

	private String uniqueQuery() {
		return "SELECT name FROM %s WHERE id = ? LIMIT %d".formatted(
			BenchmarkFixture.qualify(TABLE), ++uncachedCounter);
	}

}
