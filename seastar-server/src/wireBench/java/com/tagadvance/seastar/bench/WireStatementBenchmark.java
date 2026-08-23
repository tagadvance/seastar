package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.server.SeaStarProtocolServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
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
 * {@link StatementBenchmark}'s core set, run over {@code seastar-server} and a stock driver instead
 * of in process, so the two can be read against each other apples to apples - same fixture, same
 * statements, same JMH settings, the only difference is a loopback socket in between.
 *
 * <p>The fixture is seeded in process, on the wrapped {@link #inProcess} session, using the same
 * bulk-load trick {@link BenchmarkFixture} uses (bypassing {@code INSERT}'s table scan) - seeding
 * 1,000 rows through the wire one {@code INSERT} at a time is setup cost, not what this measures.
 * Every {@code @Benchmark} method below runs against {@link #session}, the driver connected through
 * the socket, which is the whole point.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class WireStatementBenchmark {

	static final int ROWS = 1_000;

	private static final String TABLE = "people";

	private static final int EXISTING_KEY = ROWS / 2;

	private SeaStarCqlSession inProcess;
	private SeaStarProtocolServer server;
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
		inProcess = (SeaStarCqlSession) BenchmarkFixture.newSession();
		BenchmarkFixture.seed(inProcess, TABLE, ROWS);
		inProcess.execute("CREATE TABLE %s (id int PRIMARY KEY, name text)"
			.formatted(BenchmarkFixture.qualify("batched")));

		server = SeaStarProtocolServer.builder().session(inProcess).build().start();
		session = connect(server.port());

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

	@TearDown(Level.Trial)
	public void tearDown() {
		session.close();
		server.close();
		inProcess.close();
	}

	/**
	 * Shortens the schema debouncer the same way {@code WireStartupProbe} does: left at its one
	 * second default, a benchmark including any DDL would mostly measure the debounce window rather
	 * than SeaStar.
	 */
	private static CqlSession connect(final int port) {
		final var config = DriverConfigLoader.programmaticBuilder()
			.withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofMillis(1))
			.build();

		return CqlSession.builder()
			.addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), port))
			.withLocalDatacenter("datacenter1")
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
	 * invocation, so steady state measures the wire round trip and a lookup that finds nothing.
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
