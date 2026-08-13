package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * The fidelity suite: one set of expectations, expressed only through the public driver API, run
 * against every backend that claims to behave like Cassandra. A divergence shows up as a failure in
 * one subclass and not another.
 *
 * <p>The suite is split into groups, one {@code Abstract<Group>FidelityTest} per concern, and a
 * backend extends every group once. Each group owns the keyspaces it writes to and no two groups
 * share one, so a backend's classes can run concurrently - against a shared node included. Methods
 * within a group still run ordered and on one thread; only classes run in parallel.
 *
 * <p>Public so a backend outside this package - another module's, in particular - can extend it.
 * Published nowhere: the test-fixtures variant is skipped in the publishing block.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class AbstractFidelityTest {

	protected abstract CqlSession createInstance();

	/**
	 * Whether this backend answers
	 * {@link com.datastax.oss.driver.api.core.cql.PreparedStatement#getResultMetadataId()} with an
	 * identifier rather than with null.
	 *
	 * <p>Not a fidelity question but a protocol one, and the driver's own contract rather than
	 * SeaStar's: the field arrived with native protocol v5, and the driver documents the method as
	 * returning null at v4 or lower whatever it is talking to. A backend reached over a v4 socket
	 * therefore has none to report, and would not have one from a real node either. The in-process
	 * session is on no protocol at all and computes a digest.
	 *
	 * @return true unless this backend is reached over a protocol older than v5
	 */
	protected boolean hasResultMetadataId() {
		return true;
	}

	protected CqlSession session;

	@BeforeEach
	void beforeEach() {
		if (session == null) {
			session = createInstance();
			initialize();
		}
	}

	/**
	 * Runs once per class, right after the session is created - where a group creates the keyspace
	 * its tests write to. {@link AbstractSessionFidelityTest} relies on it selecting nothing: a
	 * test there asserts on a session no {@code USE} has touched.
	 */
	protected void initialize() {
	}

	@AfterAll
	void afterAll() {
		if (session != null) {
			session.close();
		}
	}

	/**
	 * Creates a keyspace if it does not exist. One copy on the one node every backend has.
	 */
	protected void createKeyspace(final String name) {
		session.execute(("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").formatted(name));
	}

	/**
	 * The one row of a query's result.
	 */
	protected Row only(final String cql) {
		final var row = session.execute(cql).one();
		assertNotNull(row, "expected exactly one row from " + cql);

		return row;
	}

	/**
	 * The values of one column of a query's result, sorted, because which order rows come back in is
	 * a different question from which rows come back.
	 */
	protected List<String> texts(final String cql) {
		return session.execute(cql).all().stream().map(row -> row.getString(0)).sorted().toList();
	}

	protected List<String> columnNames(final String cql) {
		return StreamSupport.stream(session.execute(cql).getColumnDefinitions().spliterator(), false)
			.map(ColumnDefinition::getName)
			.map(CqlIdentifier::asInternal)
			.toList();
	}

	/**
	 * Asserts that a statement is rejected as invalid and that the message says what was at fault.
	 * The wording is Cassandra's own and differs between versions; the name does not.
	 */
	protected void assertInvalid(final String cql, final String named) {
		final var error = assertThrows(InvalidQueryException.class, () -> session.execute(cql),
			"expected to be rejected: " + cql);
		assertTrue(error.getMessage().contains(named),
			"expected the message for [%s] to name %s but it was: %s".formatted(cql, named,
				error.getMessage()));
	}

	protected static void assertMentions(final String expected, final Throwable thrown) {
		assertTrue(thrown.getMessage().toLowerCase().contains(expected.toLowerCase()),
			"%s should name %s but said: %s".formatted(thrown.getClass().getSimpleName(), expected,
				thrown.getMessage()));
	}

	protected static String md5(final String text) {
		try {
			return hex(ByteBuffer.wrap(
				MessageDigest.getInstance("MD5").digest(text.getBytes(StandardCharsets.UTF_8))));
		} catch (final NoSuchAlgorithmException e) {
			throw new IllegalStateException("MD5 is required of every JRE", e);
		}
	}

	protected static String hex(final ByteBuffer buffer) {
		final var bytes = new byte[buffer.remaining()];
		buffer.duplicate().get(bytes);
		final var hex = new StringBuilder(bytes.length * 2);
		for (final var value : bytes) {
			hex.append("%02x".formatted(value));
		}

		return hex.toString();
	}

}
