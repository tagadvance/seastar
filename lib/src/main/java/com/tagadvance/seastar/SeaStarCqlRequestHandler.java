package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandler;
import com.tagadvance.seastar.handlers.BatchHandler;
import com.tagadvance.seastar.handlers.CqlHandlerRegistry;
import com.tagadvance.seastar.handlers.CreateKeyspaceHandler;
import com.tagadvance.seastar.handlers.CreateTableHandler;
import com.tagadvance.seastar.handlers.CreateTypeHandler;
import com.tagadvance.seastar.handlers.DeleteHandler;
import com.tagadvance.seastar.handlers.DropKeyspaceHandler;
import com.tagadvance.seastar.handlers.DropTableHandler;
import com.tagadvance.seastar.handlers.InsertHandler;
import com.tagadvance.seastar.handlers.SelectHandler;
import com.tagadvance.seastar.handlers.TruncateHandler;
import com.tagadvance.seastar.handlers.UpdateHandler;
import com.tagadvance.seastar.handlers.UseKeyspaceHandler;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SeaStarCqlRequestHandler} is analogous to {@link CqlRequestHandler}.
 */
@ThreadSafe
public class SeaStarCqlRequestHandler {

	private static final Logger logger = LoggerFactory.getLogger(SeaStarCqlRequestHandler.class);

	private final Statement<?> initialStatement;
	private final SeaStarCqlSession session;
	private final SeaStarDriverContext context;
	private final RequestTracker requestTracker;
	private final List<Throwable> errors;

	private final CqlHandlerRegistry registry;

	protected SeaStarCqlRequestHandler(final Statement<?> statement,
		final SeaStarCqlSession session, final SeaStarDriverContext context) {
		this.initialStatement = statement;
		this.session = session;
		this.context = context;
		this.requestTracker = context.getRequestTracker();
		this.errors = new LinkedList<>();
		this.registry = new CqlHandlerRegistry(context.getSessionName(),
			new CreateKeyspaceHandler(), new UseKeyspaceHandler(session::setKeyspace),
			new CreateTypeHandler(session::getKeyspace),
			new CreateTableHandler(session::getKeyspace),
			new DropTableHandler(session::getKeyspace),
			new DropKeyspaceHandler(session::getKeyspace, session::setKeyspace),
			new InsertHandler(session::getKeyspace),
			new UpdateHandler(session::getKeyspace),
			new DeleteHandler(session::getKeyspace),
			new TruncateHandler(session::getKeyspace),
			new BatchHandler(this::registry),
			new SelectHandler());
	}

	private CqlHandlerRegistry registry() {
		return registry;
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

		final CQLStatement.Raw raw;
		try {
			raw = QueryProcessor.parseStatement(query);
		} catch (final Exception e) {
			return CompletableFuture.failedStage(e);
		}

		final var node = context.getNode();
		final var executionInfo = new SeaStarExecutionInfo(node, statement);

		if (requireModification && !(raw instanceof ModificationStatement.Parsed)) {
			return CompletableFuture.failedStage(new InvalidQueryException(node,
				"Only INSERT, UPDATE and DELETE statements are allowed in batches"));
		}

		return registry.processorFor(raw).processCql(context, executionInfo, raw, values);
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

//	private void logServerWarnings(Statement<?> statement, DriverExecutionProfile executionProfile,
//		List<String> warnings) {
//		// use the RequestLogFormatter to format the query
//		StringBuilder statementString = new StringBuilder();
//		context.getRequestLogFormatter()
//			.appendRequest(statement,
//				executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
//					RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH),
//				executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_VALUES,
//					RequestLogger.DEFAULT_REQUEST_LOGGER_SHOW_VALUES),
//				executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES,
//					RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUES),
//				executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
//					RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH), statementString);
//		// log each warning separately
//		warnings.forEach((warning) -> LOG.warn("Query '{}' generated server side warning(s): {}",
//			statementString, warning));
//	}

//	private ExecutionInfo buildExecutionInfo(NodeResponseCallback callback, Result resultMessage,
//		Frame responseFrame, boolean schemaInAgreement) {
//		ByteBuffer pagingState =
//			(resultMessage instanceof Rows) ? ((Rows) resultMessage).getMetadata().pagingState
//				: null;
//		return new SeaStarExecutionInfo(callback.statement, callback.node,
//			startedSpeculativeExecutionsCount.get(), callback.execution, errors, pagingState,
//			responseFrame, schemaInAgreement, session, context, executionProfile);
//	}
//
//	private void setFinalError(Statement<?> statement, Throwable error, Node node, int execution) {
//		if (error instanceof DriverException de) {
//			de.setExecutionInfo(
//				new SeaStarExecutionInfo(statement, execution, errors, context, session, context));
//		}
//		if (result.completeExceptionally(error)) {
//			if (!(requestTracker instanceof NoopRequestTracker)) {
//				requestTracker.onError(statement, error, 0, executionProfile, node,
//					handlerLogPrefix);
//			}
//		}
//	}

}
