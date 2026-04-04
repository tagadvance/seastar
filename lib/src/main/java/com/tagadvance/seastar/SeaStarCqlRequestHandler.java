package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.tracker.RequestIdGenerator;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandler;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SeaStarCqlRequestHandler} is analogous to {@link CqlRequestHandler}.
 */
@ThreadSafe
public class SeaStarCqlRequestHandler {

	private static final Logger LOG = LoggerFactory.getLogger(SeaStarCqlRequestHandler.class);

	protected final CompletableFuture<AsyncResultSet> result = new CompletableFuture<>();

	private final String handlerLogPrefix;
	private final Statement<?> initialStatement;
	private final SeaStarCqlSession session;
	private final CqlIdentifier keyspace;
	private final SeaStarDriverContext context;
	private final RequestTracker requestTracker;
	private final Optional<RequestIdGenerator> requestIdGenerator;
	private volatile List<Throwable> errors;
	private final String sessionName;
	private final String sessionRequestId;

	protected SeaStarCqlRequestHandler(final Statement<?> statement,
		final SeaStarCqlSession session, final SeaStarDriverContext context,
		final String sessionName) {
		this.requestIdGenerator = context.getRequestIdGenerator();
		this.sessionName = sessionName;
		this.sessionRequestId = this.requestIdGenerator.map(RequestIdGenerator::getSessionRequestId)
			.orElse(Integer.toString(this.hashCode()));
		this.handlerLogPrefix = "%s|%s".formatted(sessionName, sessionRequestId);
		LOG.trace("[{}] Creating new handler for request {}", handlerLogPrefix, statement);

		this.initialStatement = statement;
		this.session = session;
		this.keyspace = session.getKeyspace().orElse(null);
		this.context = context;

		this.requestTracker = context.getRequestTracker();
	}

	public CompletionStage<AsyncResultSet> handle() {
		Queue<List<ByteBuffer>> data = new LinkedList<>();

		final var asyncResultSet = new AsyncResultSet() {
			@Override
			@NonNull
			public ColumnDefinitions getColumnDefinitions() {
				return EmptyColumnDefinitions.INSTANCE; // FIXME: default
			}

			@Override
			@NonNull
			public ExecutionInfo getExecutionInfo() {
				return new SeaStarExecutionInfo(initialStatement, errors, session, context);
			}

			@Override
			public int remaining() {
				return data.size();
			}

			@Override
			@NonNull
			public Iterable<Row> currentPage() {
				return data.stream()
					.map(rowData -> new DefaultRow(getColumnDefinitions(), rowData, context))
					.map(Row.class::cast)
					.toList();
			}

			@Override
			public boolean hasMorePages() {
				// SeaStar always returns all data on the first page to keep things simple
				return false;
			}

			@Override
			@NonNull
			public CompletionStage<AsyncResultSet> fetchNextPage()
				throws IllegalStateException {
				return CompletableFuture.failedFuture(
					new IllegalStateException("No more pages"));
			}

			@Override
			public boolean wasApplied() {
				return true; // TODO: detect errors
			}
		};

		// TODO: simple statement
		// TODO: batch statement
		if (initialStatement instanceof SimpleStatement simpleStatement) {
			final var query = simpleStatement.getQuery();
			final var raw = QueryProcessor.parseStatement(query);
			if (raw instanceof CreateKeyspaceStatement.Raw statement) {
				final var name = CqlIdentifier.fromInternal(statement.keyspaceName);
				final var ifNotExists = getDeclaredField(statement, "ifNotExists",
					Boolean.class).orElse(false);
				context.node.getSeaStarKeyspace(name).ifPresentOrElse(existing -> {
					if (ifNotExists) {
						LOG.debug("Keyspace {} already exists, skipping creation", name);
					} else {
						throw new AlreadyExistsException(
							"Keyspace %s already exists".formatted(name));
					}
				}, () -> context.node.newSeaStarKeyspace(name));

				return CompletableFuture.completedStage(asyncResultSet);
			}
		}
		if (initialStatement instanceof SeaStarBoundStatement boundStatement) {
			final var preparedStatement = boundStatement.getPreparedStatement();
			final var query = preparedStatement.getQuery();
			final var raw = QueryProcessor.parseStatement(query);
			if (raw instanceof SelectStatement.RawStatement selectStatement) {
				if (selectStatement.name() != null && selectStatement.selectClause.isEmpty()
					&& selectStatement.whereClause.equals(WhereClause.empty())) {
					final var isDistinct = selectStatement.parameters.isDistinct;
					// TODO: processs simple select
					System.gc();
				}
			}

			return CompletableFuture.completedStage(asyncResultSet);
		}

		throw new UnsupportedOperationException(
			"Statement of type %s is not currently supported".formatted(
				initialStatement.getClass().getSimpleName()));
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

	@SuppressWarnings("unchecked")
	private static <V> Optional<V> getDeclaredField(final Object o, final String name,
		final Class<V> returnType) {
		try {
			final var field = o.getClass().getDeclaredField(name);
			field.setAccessible(true);
			final var value = field.get(o);
			if (returnType.isInstance(value)) {
				return Optional.of((V) value);
			}
		} catch (final NoSuchFieldException | IllegalAccessException e) {
			LOG.error(e.getMessage(), e);
		}

		return Optional.empty();
	}

}
