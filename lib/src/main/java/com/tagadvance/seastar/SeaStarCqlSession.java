package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.DefaultAsyncResultSet;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import com.datastax.oss.driver.internal.core.cql.SinglePageResultSet;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;
import org.jspecify.annotations.NonNull;

public class SeaStarCqlSession implements CqlSession {

	private final SeaStarDriverContext context;
	private final AtomicReference<CqlIdentifier> keyspace = new AtomicReference<>();
	private final Metadata metadata = new SeaStarMetadata();

	SeaStarCqlSession(final SeaStarDriverContext context, final CqlIdentifier keyspace) {
		this.context = requireNonNull(context, "context must not be null");
		this.keyspace.set(keyspace);
	}

	@Override
	@NonNull
	public String getName() {
		return context.getSessionName();
	}

	@Override
	@NonNull
	public Metadata getMetadata() {
		return metadata;
	}

	@Override
	public boolean isSchemaMetadataEnabled() {
		return true;
	}

	@Override
	@NonNull
	public CompletionStage<Metadata> setSchemaMetadataEnabled(final Boolean newValue) {
		return CompletableFuture.completedFuture(metadata);
	}

	@Override
	@NonNull
	public CompletionStage<Metadata> refreshSchemaAsync() {
		// FIXME: perform refresh
		return CompletableFuture.completedFuture(metadata);
	}

	@Override
	@NonNull
	public CompletionStage<Boolean> checkSchemaAgreementAsync() {
		return CompletableFuture.completedFuture(true);
	}

	@Override
	@NonNull
	public DriverContext getContext() {
		return context;
	}

	@Override
	@NonNull
	public Optional<CqlIdentifier> getKeyspace() {
		return Optional.of(keyspace).map(AtomicReference::get);
	}

	@Override
	@NonNull
	public Optional<Metrics> getMetrics() {
		return Optional.empty();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <RequestT extends Request, ResultT> ResultT execute(@NonNull final RequestT request,
		final @NonNull GenericType<ResultT> resultType) {
		if (Objects.equals(resultType, PrepareRequest.SYNC) || Objects.equals(resultType,
			PrepareRequest.ASYNC)) {
			final PreparedStatement preparedStatement;
			if (request instanceof SimpleStatement statement) {
				final var query = statement.getQuery();
				preparedStatement = new SeaStarPreparedStatement(query);
			} else if (request instanceof PrepareRequest prepareRequest) {
				preparedStatement = new SeaStarPreparedStatement(prepareRequest);
			} else {
				throw new UnsupportedOperationException(
					"Unsupported request type for prepare: " + request.getClass().getName());
			}

			return (ResultT) (Objects.equals(resultType, PrepareRequest.ASYNC)
				? CompletableFuture.completedStage(preparedStatement) : preparedStatement);
		}

		if (Objects.equals(resultType, Statement.ASYNC) || Objects.equals(resultType,
			Statement.SYNC)) {
			final AsyncResultSet resultSet;
			if (request instanceof SimpleStatement statement) {
				// TODO; process query and populate results
				final var definitions = DefaultColumnDefinitions.valueOf(List.of()); // TODO
				final var executionInfo = new SeaStarExecutionInfo(statement);
				final var data = new LinkedList<List<java.nio.ByteBuffer>>(); // TODO
				resultSet = new DefaultAsyncResultSet(definitions, executionInfo, data,
					SeaStarCqlSession.this, context);
			} else if (request instanceof SeaStarBoundStatement boundStatement) {
				// TODO; process query and populate results
				final var definitions = DefaultColumnDefinitions.valueOf(List.of()); // TODO
				final var executionInfo = new SeaStarExecutionInfo(boundStatement);
				final var data = new LinkedList<List<java.nio.ByteBuffer>>(); // TODO
				resultSet = new DefaultAsyncResultSet(definitions, executionInfo, data,
					SeaStarCqlSession.this, context);
			} else if (request instanceof BatchStatement batchStatement) {
				// TODO; process query and populate results
				final var batchableStatements = extractQuery(batchStatement);

				final var definitions = DefaultColumnDefinitions.valueOf(List.of());
				final var executionInfo = new SeaStarExecutionInfo(batchStatement);
				final var data = new LinkedList<List<java.nio.ByteBuffer>>(); // TODO
				resultSet = new DefaultAsyncResultSet(definitions, executionInfo, data,
					SeaStarCqlSession.this, context);
			} else {
				throw new UnsupportedOperationException(
					"Unsupported request type for prepare: " + request.getClass().getName());
			}

			return (ResultT) (Objects.equals(resultType, Statement.SYNC) ? new SinglePageResultSet(
				resultSet) : CompletableFuture.completedStage(resultSet));
		}

		throw new UnsupportedOperationException(); // TODO better description
	}

	private List<BatchableStatement<?>> extractQuery(final BatchStatement batchStatement) {
		final var iterator = batchStatement.iterator();
		final var spliterator = Spliterators.spliteratorUnknownSize(iterator,
			0);
		final var stream = StreamSupport.stream(spliterator, false);

		return stream.toList();
	}

	@Override
	@NonNull
	public CompletionStage<Void> closeFuture() {
		return CompletableFuture.completedStage(null);
	}

	@Override
	@NonNull
	public CompletionStage<Void> closeAsync() {
		return CompletableFuture.completedStage(null);
	}

	@Override
	@NonNull
	public CompletionStage<Void> forceCloseAsync() {
		return CompletableFuture.completedStage(null);
	}

	@NonNull
	public static SeaStarCqlSessionBuilder builder() {
		return new SeaStarCqlSessionBuilder();
	}

	private final class SeaStarPreparedStatement implements PreparedStatement {

		private final ByteBuffer id;
		private final PrepareRequest prepareRequest;

		public SeaStarPreparedStatement(final SimpleStatement statement) {
			this(new DefaultPrepareRequest(statement));
		}

		public SeaStarPreparedStatement(final String query) {
			this(new DefaultPrepareRequest(query));
		}

		public SeaStarPreparedStatement(final PrepareRequest request) {
			this.id = ByteBuffer.wrap(UUID.randomUUID().toString().getBytes());
			this.prepareRequest = requireNonNull(request, "request must not be null");
		}

		@Override
		@NonNull
		public ByteBuffer getId() {
			return id;
		}

		@Override
		@NonNull
		public String getQuery() {
			return prepareRequest.getQuery();
		}

		@Override
		@NonNull
		public ColumnDefinitions getVariableDefinitions() {
			// FIXME
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public List<Integer> getPartitionKeyIndices() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ByteBuffer getResultMetadataId() {
			final var hash = getResultSetDefinitions().hashCode();

			return ByteBuffer.allocate(4).putInt(hash);
		}

		@Override
		@NonNull
		public ColumnDefinitions getResultSetDefinitions() {
			// FIXME
			throw new UnsupportedOperationException();
		}

		@Override
		public void setResultMetadata(final @NonNull ByteBuffer newResultMetadataId,
			final @NonNull ColumnDefinitions newResultSetDefinitions) {
			// FIXME
			throw new UnsupportedOperationException();
		}

		@Override
		public @NonNull BoundStatement bind(final Object @NonNull ... values) {
			return new SeaStarBoundStatement(this);
		}

		@Override
		@NonNull
		public BoundStatementBuilder boundStatementBuilder(final Object @NonNull ... values) {
			// FIXME
			throw new UnsupportedOperationException();
		}

	}

	private final class SeaStarBoundStatement implements BoundStatement {

		private final PreparedStatement preparedStatement;
		private final Object[] values;
		private final AtomicLong timestamp = new AtomicLong();

		private SeaStarBoundStatement(final @NonNull PreparedStatement preparedStatement,
			final Object @NonNull ... values) {
			this.preparedStatement = requireNonNull(preparedStatement,
				"preparedStatement must not be null");
			this.values = requireNonNull(values, "values must not be null");
			final long timestamp = context.getTimestampGenerator().next();
			setQueryTimestamp(timestamp);
		}

		@Override
		@NonNull
		public PreparedStatement getPreparedStatement() {
			return preparedStatement;
		}

		@Override
		@NonNull
		public List<ByteBuffer> getValues() {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setExecutionProfileName(final String newConfigProfileName) {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setExecutionProfile(final DriverExecutionProfile newProfile) {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setRoutingKeyspace(final CqlIdentifier newRoutingKeyspace) {
			// FIXME
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setNode(final Node node) {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setRoutingKey(final ByteBuffer newRoutingKey) {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setRoutingToken(final Token newRoutingToken) {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setCustomPayload(
			final @NonNull Map<String, ByteBuffer> newCustomPayload) {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setIdempotent(final Boolean newIdempotence) {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setTracing(final boolean newTracing) {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getQueryTimestamp() {
			return timestamp.get();
		}

		@Override
		@NonNull
		public BoundStatement setQueryTimestamp(final long newTimestamp) {
			timestamp.set(newTimestamp);

			return this;
		}

		@Override
		@NonNull
		public BoundStatement setTimeout(final Duration newTimeout) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ByteBuffer getPagingState() {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public BoundStatement setPagingState(final ByteBuffer newPagingState) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getPageSize() {
			return Integer.MAX_VALUE;
		}

		@Override
		@NonNull
		public BoundStatement setPageSize(final int newPageSize) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ConsistencyLevel getConsistencyLevel() {
			return ConsistencyLevel.LOCAL_ONE;
		}

		@Override
		@NonNull
		public BoundStatement setConsistencyLevel(final ConsistencyLevel newConsistencyLevel) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ConsistencyLevel getSerialConsistencyLevel() {
			return ConsistencyLevel.LOCAL_SERIAL;
		}

		@Override
		@NonNull
		public BoundStatement setSerialConsistencyLevel(
			final ConsistencyLevel newSerialConsistencyLevel) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isTracing() {
			return false;
		}

		@Override
		public int firstIndexOf(final @NonNull String name) {
			return 0;
		}

		@Override
		public int firstIndexOf(final @NonNull CqlIdentifier id) {
			return 0;
		}

		@Override
		public ByteBuffer getBytesUnsafe(final int i) {
			return null;
		}

		@Override
		@NonNull
		public BoundStatement setBytesUnsafe(final int i, final ByteBuffer v) {
			// TODO
			return this;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		@NonNull
		public DataType getType(final int i) {
			return null;
		}

		@Override
		@NonNull
		public CodecRegistry codecRegistry() {
			return context.getCodecRegistry();
		}

		@Override
		@NonNull
		public ProtocolVersion protocolVersion() {
			return context.getProtocolVersion();
		}

		@Override
		public String getExecutionProfileName() {
			throw new UnsupportedOperationException();
		}

		@Override
		public DriverExecutionProfile getExecutionProfile() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CqlIdentifier getRoutingKeyspace() {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		public ByteBuffer getRoutingKey() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Token getRoutingToken() {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public Map<String, ByteBuffer> getCustomPayload() {
			// FIXME
			return Map.of();
		}

		@Override
		public Boolean isIdempotent() {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		public Duration getTimeout() {
			return Duration.ZERO;
		}

		@Override
		public Node getNode() {
			throw new UnsupportedOperationException();
		}

	}

	private static final class SeaStarExecutionInfo implements ExecutionInfo {

		private final Statement<?> statement;

		private SeaStarExecutionInfo(final Statement<?> statement) {
			this.statement = requireNonNull(statement, "statement must not be null");
		}

		@Override
		@NonNull
		public Statement<?> getStatement() {
			return statement;
		}

		@Override
		public Node getCoordinator() {
			return null;
		}

		@Override
		public int getSpeculativeExecutionCount() {
			return 0;
		}

		@Override
		public int getSuccessfulExecutionIndex() {
			return 0;
		}

		@Override
		@NonNull
		public List<Entry<Node, Throwable>> getErrors() {
			return List.of();
		}

		@Override
		public ByteBuffer getPagingState() {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public List<String> getWarnings() {
			return List.of();
		}

		@Override
		@NonNull
		public Map<String, ByteBuffer> getIncomingPayload() {
			// TODO
			return Map.of();
		}

		@Override
		public boolean isSchemaInAgreement() {
			return true;
		}

		@Override
		public UUID getTracingId() {
			throw new UnsupportedOperationException();
		}

		@Override
		@NonNull
		public CompletionStage<QueryTrace> getQueryTraceAsync() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getResponseSizeInBytes() {
			return -1;
		}

		@Override
		public int getCompressedResponseSizeInBytes() {
			return -1;
		}

	}

}
