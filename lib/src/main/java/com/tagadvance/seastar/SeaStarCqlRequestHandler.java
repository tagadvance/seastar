package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandler;
import com.tagadvance.seastar.handlers.CqlHandlerRegistry;
import com.tagadvance.seastar.handlers.CqlParsers;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;

/**
 * {@link SeaStarCqlRequestHandler} is analogous to {@link CqlRequestHandler}.
 */
@ThreadSafe
public class SeaStarCqlRequestHandler {

	private final Statement<?> initialStatement;
	private final SeaStarDriverContext context;
	private final CqlHandlerRegistry registry;

	protected SeaStarCqlRequestHandler(final Statement<?> statement,
		final SeaStarCqlSession session, final SeaStarDriverContext context) {
		this.initialStatement = statement;
		this.context = context;
		this.registry = session.handlerRegistry();
	}

	public CompletionStage<AsyncResultSet> handle() {
		if (initialStatement instanceof BatchStatement batch) {
			return handleBatch(batch);
		}

		return dispatch(initialStatement, false);
	}

	// Apply each child statement in sequence under the table locks, then return a void result whose
	// wasApplied() is true, matching a non-conditional batch on a live cluster.
	private CompletionStage<AsyncResultSet> handleBatch(final BatchStatement batch) {
		CompletionStage<AsyncResultSet> chain = CompletableFuture.completedStage(null);
		for (final var child : batch) {
			chain = chain.thenCompose(ignored -> dispatch(child, true));
		}

		final var executionInfo = new SeaStarExecutionInfo(context.getNode(), batch);

		return chain.thenApply(ignored -> SeaStarAsyncResultSet.empty(executionInfo));
	}

	private CompletionStage<AsyncResultSet> dispatch(final Statement<?> statement,
		final boolean requireModification) {
		final String query;
		final Object[] values;
		if (statement instanceof SimpleStatement simpleStatement) {
			query = simpleStatement.getQuery();
			values = new Object[]{};
		} else if (statement instanceof SeaStarBoundStatement boundStatement) {
			final var preparedStatement = boundStatement.getPreparedStatement();
			query = preparedStatement.getQuery();
			values = boundStatement.getBoundValues();
		} else if (statement instanceof BoundStatement boundStatement) {
			final var preparedStatement = boundStatement.getPreparedStatement();
			query = preparedStatement.getQuery();
			values = decode(boundStatement, preparedStatement.getVariableDefinitions());
		} else {
			throw new UnsupportedOperationException(
				"Statement of type %s is not currently supported".formatted(
					statement.getClass().getSimpleName()));
		}

		final var node = context.getNode();
		final var executionInfo = new SeaStarExecutionInfo(node, statement);

		final CQLStatement.Raw raw;
		try {
			raw = CqlParsers.parse(node, query);
		} catch (final Exception e) {
			attach(executionInfo, e);

			return CompletableFuture.failedStage(e);
		}

		if (requireModification && !(raw instanceof ModificationStatement.Parsed)) {
			final var error = new InvalidQueryException(node,
				"Only INSERT, UPDATE and DELETE statements are allowed in batches");
			attach(executionInfo, error);

			return CompletableFuture.failedStage(error);
		}

		try {
			return registry.processorFor(raw, executionInfo)
				.processCql(context, executionInfo, raw, values)
				.whenComplete((result, error) -> attach(executionInfo, error));
		} catch (final RuntimeException e) {
			attach(executionInfo, e);

			// A handler that throws rather than returning a failed stage must not escape the async
			// path; CompletionStage forbids it, and the sync processor is what turns this back into
			// a throw.
			return CompletableFuture.failedStage(e);
		}
	}

	private Object[] decode(final BoundStatement statement, final ColumnDefinitions variables) {
		final var codecRegistry = context.getCodecRegistry();
		final var protocolVersion = context.getProtocolVersion();
		final var values = new Object[variables.size()];
		for (int i = 0; i < values.length; i++) {
			final var bytes = statement.getBytesUnsafe(i);
			values[i] = bytes == null ? null
				: codecRegistry.codecFor(variables.get(i).getType()).decode(bytes, protocolVersion);
		}

		return values;
	}

	/**
	 * Populates {@link DriverException#getExecutionInfo()} the way {@link CqlRequestHandler} does, so
	 * that a failure rethrown to the caller carries the same context it would on a live cluster.
	 */
	private static void attach(final ExecutionInfo executionInfo, final Throwable error) {
		final var unwrapped = error instanceof CompletionException ? error.getCause() : error;
		if (unwrapped instanceof DriverException driverException) {
			driverException.setExecutionInfo(executionInfo);
		}
	}

}
