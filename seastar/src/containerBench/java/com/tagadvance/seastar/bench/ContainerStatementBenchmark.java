package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletionStage;
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
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * {@link StatementBenchmark}'s core set against a real {@code cassandra:5.0.8} node via
 * TestContainers, so the wire and container numbers can sit next to the in-process ones. Run
 * through {@code org.openjdk.jmh.Main} against the {@code containerBench} classpath, not through
 * the {@code me.champeau.jmh} plugin, which only knows about the {@code jmh} source set - see
 * HANDOVER trap 4 for why TestContainers cannot share a classpath with the pinned driver.
 *
 * <p>The fixture is seeded through the driver, not the storage-model shortcut
 * {@link BenchmarkFixture} uses: there is no bypassing {@code INSERT} against a real node. 1 000
 * rows are pushed with a bounded window of async requests so setup does not dominate the run.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class ContainerStatementBenchmark {

	static final int ROWS = 1_000;

	private static final String IMAGE = "cassandra:5.0.8";

	private static final String KEYSPACE = "bench";

	private static final String TABLE = "people";

	private static final int EXISTING_KEY = ROWS / 2;

	private static final int ASYNC_WINDOW = 200;

	private CassandraContainer container;
	private CqlSession session;
	private PreparedStatement insert;
	private PreparedStatement update;
	private PreparedStatement delete;
	private PreparedStatement pointSelect;
	private BoundStatement boundInsert;
	private BatchStatement batch;
	private SimpleStatement scan;

	@Setup(Level.Trial)
	public void setUp() {
		container = new CassandraContainer(DockerImageName.parse(IMAGE));
		container.start();
		session = connect(container);

		session.execute(("CREATE KEYSPACE %s WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").formatted(KEYSPACE));
		session.execute(
			"CREATE TABLE %s.%s (id int PRIMARY KEY, name text, age int)".formatted(KEYSPACE, TABLE));
		session.execute("CREATE TABLE %s.batched (id int PRIMARY KEY, name text)".formatted(KEYSPACE));
		seed();

		final var qualified = KEYSPACE + "." + TABLE;
		insert = session.prepare("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)".formatted(qualified));
		update = session.prepare("UPDATE %s SET name = ? WHERE id = ?".formatted(qualified));
		delete = session.prepare("DELETE FROM %s WHERE id = ?".formatted(qualified));
		pointSelect = session.prepare("SELECT name FROM %s WHERE id = ?".formatted(qualified));
		boundInsert = insert.bind(EXISTING_KEY, "bound", 1);
		scan = SimpleStatement.newInstance("SELECT * FROM " + qualified);

		final var builder = BatchStatement.builder(DefaultBatchType.LOGGED);
		IntStream.range(0, 100)
			.mapToObj(id -> SimpleStatement.newInstance(
				"INSERT INTO %s.batched (id, name) VALUES (%d, 'batched')".formatted(KEYSPACE, id)))
			.forEach(builder::addStatement);
		batch = builder.build();
	}

	private void seed() {
		final var prepared = session.prepare(
			"INSERT INTO %s.%s (id, name, age) VALUES (?, ?, ?)".formatted(KEYSPACE, TABLE));
		final var inFlight = new ArrayDeque<CompletionStage<AsyncResultSet>>();
		for (int id = 0; id < ROWS; id++) {
			inFlight.add(session.executeAsync(prepared.bind(id, "name-" + id, id % 100)));
			if (inFlight.size() >= ASYNC_WINDOW) {
				inFlight.poll().toCompletableFuture().join();
			}
		}
		while (!inFlight.isEmpty()) {
			inFlight.poll().toCompletableFuture().join();
		}
	}

	@TearDown(Level.Trial)
	public void tearDown() {
		session.close();
		container.stop();
	}

	/**
	 * Shortens the schema debouncer to 1 ms, the same as the wire benchmark, so the setup DDL above
	 * does not pay the driver's one-second metadata refresh window on top of a real node's own DDL
	 * cost.
	 */
	private static CqlSession connect(final CassandraContainer container) {
		final var config = DriverConfigLoader.programmaticBuilder()
			.withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofMillis(1))
			.build();

		return CqlSession.builder()
			.addContactPoint(new InetSocketAddress(container.getHost(), container.getMappedPort(9042)))
			.withLocalDatacenter(container.getLocalDatacenter())
			.withConfigLoader(config)
			.build();
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
	 * As in {@link StatementBenchmark#deleteByPrimaryKey()}: the row is gone after the first
	 * invocation, so steady state measures the round trip and a lookup that finds nothing.
	 */
	@Benchmark
	public Object deleteByPrimaryKey() {
		return session.execute(delete.bind(EXISTING_KEY));
	}

	@Benchmark
	public ResultSet batch100() {
		return session.execute(batch);
	}

}
