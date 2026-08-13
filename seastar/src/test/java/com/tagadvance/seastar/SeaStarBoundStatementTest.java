package com.tagadvance.seastar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A bound statement's transport settings (execution profile, node,
 * routing token, tracing, timeout, paging, page size, consistency) are accepted and stored rather
 * than rejected, even though SeaStar's query engine never consults them - a caller sharing
 * builder/statement configuration code with a real session should not have to special-case SeaStar.
 */
class SeaStarBoundStatementTest {

	private SeaStarBoundStatement bind() {
		try (final var session = SeaStarCqlSession.builder().build()) {
			session.execute("CREATE KEYSPACE ks WITH replication = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE ks.t (id int PRIMARY KEY)");
			final var prepared = session.prepare("SELECT * FROM ks.t WHERE id = ?");

			return (SeaStarBoundStatement) prepared.bind(1);
		}
	}

	@Test
	@DisplayName("setTracing/isTracing round-trip instead of throwing")
	void testTracing() {
		final var bound = bind();
		bound.setTracing(true);

		assertTrue(bound.isTracing());
	}

	@Test
	@DisplayName("setTimeout/getTimeout round-trip instead of throwing")
	void testTimeout() {
		final var bound = bind();
		final var timeout = Duration.ofSeconds(5);
		bound.setTimeout(timeout);

		assertEquals(timeout, bound.getTimeout());
	}

	@Test
	@DisplayName("setPagingState/getPagingState round-trip instead of throwing")
	void testPagingState() {
		final var bound = bind();
		final var state = ByteBuffer.wrap(new byte[] {1, 2, 3});
		bound.setPagingState(state);

		assertEquals(state, bound.getPagingState());
	}

	@Test
	@DisplayName("setPageSize/getPageSize round-trip instead of throwing")
	void testPageSize() {
		final var bound = bind();
		bound.setPageSize(10);

		assertEquals(10, bound.getPageSize());
	}

	@Test
	@DisplayName("setConsistencyLevel/getConsistencyLevel round-trip instead of throwing")
	void testConsistencyLevel() {
		final var bound = bind();
		bound.setConsistencyLevel(ConsistencyLevel.QUORUM);

		assertEquals(ConsistencyLevel.QUORUM, bound.getConsistencyLevel());
	}

	@Test
	@DisplayName("setSerialConsistencyLevel/getSerialConsistencyLevel round-trip instead of throwing")
	void testSerialConsistencyLevel() {
		final var bound = bind();
		bound.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);

		assertEquals(ConsistencyLevel.SERIAL, bound.getSerialConsistencyLevel());
	}

	@Test
	@DisplayName("setExecutionProfileName/getExecutionProfileName round-trip instead of throwing")
	void testExecutionProfileName() {
		final var bound = bind();
		bound.setExecutionProfileName("slow");

		assertEquals("slow", bound.getExecutionProfileName());
	}

	@Test
	@DisplayName("setRoutingToken/getRoutingToken round-trip instead of throwing")
	void testRoutingToken() {
		final var bound = bind();
		final var token = mock(Token.class);
		bound.setRoutingToken(token);

		assertEquals(token, bound.getRoutingToken());
	}

	@Test
	@DisplayName("setNode/getNode round-trip instead of throwing")
	void testNode() {
		final var bound = bind();
		final var node = mock(Node.class);
		bound.setNode(node);

		assertEquals(node, bound.getNode());
	}

}
