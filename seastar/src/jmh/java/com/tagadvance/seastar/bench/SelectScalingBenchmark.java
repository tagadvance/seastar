package com.tagadvance.seastar.bench;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Exposes the O(rows) behavior of every SELECT: there is no index on the partition key, so a point
 * lookup scans the whole table, and a full scan additionally deserializes and re-serializes every
 * row.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class SelectScalingBenchmark {

	private static final String TABLE = "scale";

	@Param({"10", "1000", "100000"})
	public int rows;

	private CqlSession session;
	private PreparedStatement pointSelect;
	private SimpleStatement scan;

	@Setup(Level.Trial)
	public void setUp() {
		session = BenchmarkFixture.newSession();
		BenchmarkFixture.seed(session, TABLE, rows);
		final var qualified = BenchmarkFixture.qualify(TABLE);
		pointSelect = session.prepare("SELECT name FROM %s WHERE id = ?".formatted(qualified));
		scan = SimpleStatement.newInstance("SELECT * FROM " + qualified);
	}

	@TearDown(Level.Trial)
	public void tearDown() {
		session.close();
	}

	@Benchmark
	public Row selectPoint() {
		return session.execute(pointSelect.bind(rows / 2)).one();
	}

	@Benchmark
	public List<Row> selectAll() {
		return session.execute(scan).all();
	}

}
