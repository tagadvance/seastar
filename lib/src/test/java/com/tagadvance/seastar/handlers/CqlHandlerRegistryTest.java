package com.tagadvance.seastar.handlers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers the fallback every statement type SeaStar knows about now bypasses: a registry with no
 * handler for a statement is what a future {@code cassandra-all} adding a statement type would
 * produce, and it must still fail as a driver exception naming the CQL rather than the parse tree.
 */
class CqlHandlerRegistryTest {

	@Test
	@DisplayName("A statement no handler claims fails as an invalid query naming the CQL statement")
	void testUnhandledStatement() {
		final var query = "ALTER TABLE ks.t ADD age int";
		final var node = mock(Node.class);
		final var executionInfo = mock(ExecutionInfo.class);
		when(executionInfo.getCoordinator()).thenReturn(node);
		when(executionInfo.getRequest()).thenReturn(SimpleStatement.newInstance(query));

		final var registry = new CqlHandlerRegistry("test");
		final var raw = CqlParsers.parse(node, query);

		final var thrown = assertThrows(InvalidQueryException.class,
			() -> registry.processorFor(raw, executionInfo));

		// The CQL name of the statement, not the parse-tree class or its identity hash.
		assertTrue(thrown.getMessage().contains("ALTER TABLE"), thrown.getMessage());
		assertTrue(thrown.getMessage().contains(query), thrown.getMessage());
		assertFalse(thrown.getMessage().contains("AlterTableStatement"), thrown.getMessage());
		assertFalse(thrown.getMessage().contains("@"), thrown.getMessage());
	}

}
