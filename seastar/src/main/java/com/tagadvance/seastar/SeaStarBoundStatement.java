package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.util.RoutingKey;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import net.jcip.annotations.NotThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * Not safe for concurrent use, unlike the driver's own {@code BoundStatement}: the real driver's
 * setters return a new immutable copy, while these mutate {@code this} in place through plain
 * fields with no lock. Bind a statement on one thread and hand it to another rather than sharing it
 * across threads that both mutate it.
 */
@NotThreadSafe
class SeaStarBoundStatement implements BoundStatement {

	private final SeaStarDriverContext context;
	private final PreparedStatement preparedStatement;
	private Object[] values;
	private final AtomicLong timestamp = new AtomicLong();
	private CqlIdentifier routingKeyspace;
	private ByteBuffer routingKey;
	private Boolean idempotent;
	private Map<String, ByteBuffer> customPayload = Map.of();
	private String executionProfileName;
	private DriverExecutionProfile executionProfile;
	private Node node;
	private Token routingToken;
	private boolean tracing;
	private Duration timeout = Duration.ZERO;
	private ByteBuffer pagingState;
	private int pageSize = Integer.MAX_VALUE;
	private ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;
	private ConsistencyLevel serialConsistencyLevel = ConsistencyLevel.LOCAL_SERIAL;

	public SeaStarBoundStatement(final SeaStarDriverContext context,
		final @NonNull PreparedStatement preparedStatement,
		final Object @NonNull ... values) {
		this.context = requireNonNull(context, "context must not be null");
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

	/**
	 * The raw Java values supplied to {@link SeaStarPreparedStatement#bind(Object...)}, positional by
	 * bind marker index. Distinct from {@link #getValues()}, which is the encoded driver form.
	 */
	public Object[] getBoundValues() {
		return values;
	}

	@Override
	@NonNull
	public List<ByteBuffer> getValues() {
		return IntStream.range(0, size())
			.mapToObj(this::getBytesUnsafe)
			.toList();
	}

	/**
	 * Stored and handed back but otherwise inert: SeaStar has no execution profiles to select
	 * between, so a caller sharing configuration code with a real session does not have to special-case
	 * this one.
	 */
	@Override
	@NonNull
	public BoundStatement setExecutionProfileName(final String newConfigProfileName) {
		this.executionProfileName = newConfigProfileName;

		return this;
	}

	/**
	 * @see #setExecutionProfileName(String)
	 */
	@Override
	@NonNull
	public BoundStatement setExecutionProfile(final DriverExecutionProfile newProfile) {
		this.executionProfile = newProfile;

		return this;
	}

	@Override
	@NonNull
	public BoundStatement setRoutingKeyspace(final CqlIdentifier newRoutingKeyspace) {
		this.routingKeyspace = newRoutingKeyspace;

		return this;
	}

	/**
	 * Stored and handed back but otherwise inert: SeaStar has a single node, so there is nothing to
	 * route to. See {@link #getNode()}.
	 */
	@Override
	@NonNull
	public BoundStatement setNode(final Node node) {
		this.node = node;

		return this;
	}

	@Override
	@NonNull
	public BoundStatement setRoutingKey(final ByteBuffer newRoutingKey) {
		this.routingKey = newRoutingKey;

		return this;
	}

	/**
	 * Stored and handed back but otherwise inert: SeaStar has no token ring, so nothing routes on it.
	 * See {@link #getRoutingToken()}.
	 */
	@Override
	@NonNull
	public BoundStatement setRoutingToken(final Token newRoutingToken) {
		this.routingToken = newRoutingToken;

		return this;
	}

	@Override
	@NonNull
	public BoundStatement setCustomPayload(
		final @NonNull Map<String, ByteBuffer> newCustomPayload) {
		this.customPayload = Map.copyOf(
			requireNonNull(newCustomPayload, "newCustomPayload must not be null"));

		return this;
	}

	@Override
	@NonNull
	public BoundStatement setIdempotent(final Boolean newIdempotence) {
		this.idempotent = newIdempotence;

		return this;
	}

	/**
	 * Stored and reflected back by {@link #isTracing()} but otherwise inert: SeaStar answers from
	 * memory on the calling thread, so there is no coordinator round trip to trace.
	 */
	@Override
	@NonNull
	public BoundStatement setTracing(final boolean newTracing) {
		this.tracing = newTracing;

		return this;
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

	/**
	 * Stored and reflected back by {@link #getTimeout()} but otherwise inert: SeaStar answers from
	 * memory, so a request never has the chance to run long enough for this to matter.
	 */
	@Override
	@NonNull
	public BoundStatement setTimeout(final Duration newTimeout) {
		this.timeout = newTimeout;

		return this;
	}

	@Override
	public ByteBuffer getPagingState() {
		return pagingState;
	}

	/**
	 * Stored and handed back but otherwise inert: SeaStar's {@code SELECT} always answers in a single
	 * page (see {@code AGENTS.md}), so there is no paging state to resume from.
	 */
	@Override
	@NonNull
	public BoundStatement setPagingState(final ByteBuffer newPagingState) {
		this.pagingState = newPagingState;

		return this;
	}

	@Override
	public int getPageSize() {
		return pageSize;
	}

	/**
	 * Stored and handed back but otherwise inert: SeaStar's {@code SELECT} always answers in a single
	 * page, so nothing consults a page size.
	 */
	@Override
	@NonNull
	public BoundStatement setPageSize(final int newPageSize) {
		this.pageSize = newPageSize;

		return this;
	}

	@Override
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	/**
	 * Stored and handed back but otherwise inert: SeaStar has no replicas to satisfy a consistency
	 * level against.
	 */
	@Override
	@NonNull
	public BoundStatement setConsistencyLevel(final ConsistencyLevel newConsistencyLevel) {
		this.consistencyLevel = newConsistencyLevel;

		return this;
	}

	@Override
	public ConsistencyLevel getSerialConsistencyLevel() {
		return serialConsistencyLevel;
	}

	/**
	 * @see #setConsistencyLevel(ConsistencyLevel)
	 */
	@Override
	@NonNull
	public BoundStatement setSerialConsistencyLevel(
		final ConsistencyLevel newSerialConsistencyLevel) {
		this.serialConsistencyLevel = newSerialConsistencyLevel;

		return this;
	}

	@Override
	public boolean isTracing() {
		return tracing;
	}

	@Override
	@NonNull
	public List<Integer> allIndicesOf(final @NonNull String name) {
		final var indices = preparedStatement.getVariableDefinitions().allIndicesOf(name);
		if (indices.isEmpty()) {
			throw new IllegalArgumentException(
				"%s is not a variable in this bound statement".formatted(name));
		}

		return indices;
	}

	@Override
	@NonNull
	public List<Integer> allIndicesOf(final @NonNull CqlIdentifier id) {
		final var indices = preparedStatement.getVariableDefinitions().allIndicesOf(id);
		if (indices.isEmpty()) {
			throw new IllegalArgumentException(
				"%s is not a variable in this bound statement".formatted(id));
		}

		return indices;
	}

	@Override
	public int firstIndexOf(final @NonNull String name) {
		final var index = preparedStatement.getVariableDefinitions().firstIndexOf(name);
		if (index < 0) {
			throw new IllegalArgumentException(
				"%s is not a variable in this bound statement".formatted(name));
		}

		return index;
	}

	@Override
	public int firstIndexOf(final @NonNull CqlIdentifier id) {
		final var index = preparedStatement.getVariableDefinitions().firstIndexOf(id);
		if (index < 0) {
			throw new IllegalArgumentException(
				"%s is not a variable in this bound statement".formatted(id));
		}

		return index;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ByteBuffer getBytesUnsafe(final int i) {
		final var value = i < values.length ? values[i] : null;
		if (value == null) {
			return null;
		}
		final var codec = codecRegistry().codecFor(getType(i));

		return codec.encode(value, protocolVersion());
	}

	@Override
	@NonNull
	public BoundStatement setBytesUnsafe(final int i, final ByteBuffer v) {
		if (i >= values.length) {
			values = Arrays.copyOf(values, i + 1);
		}
		values[i] = codecRegistry().codecFor(getType(i)).decode(v, protocolVersion());

		return this;
	}

	@Override
	public int size() {
		return preparedStatement.getVariableDefinitions().size();
	}

	@Override
	@NonNull
	public DataType getType(final int i) {
		return preparedStatement.getVariableDefinitions().get(i).getType();
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
		return executionProfileName;
	}

	@Override
	public DriverExecutionProfile getExecutionProfile() {
		return executionProfile;
	}

	@Override
	public CqlIdentifier getRoutingKeyspace() {
		return routingKeyspace;
	}

	/**
	 * An explicitly set routing key, otherwise the partition key assembled from the bind markers
	 * identified by {@link PreparedStatement#getPartitionKeyIndices()}. Null when the partition key
	 * cannot be determined, either because a component is not a bind marker or because one is unset.
	 */
	@Override
	public ByteBuffer getRoutingKey() {
		if (routingKey != null) {
			return routingKey;
		}

		final var indices = preparedStatement.getPartitionKeyIndices();
		if (indices.isEmpty()) {
			return null;
		}

		final var components = indices.stream()
			.map(this::getBytesUnsafe)
			.toArray(ByteBuffer[]::new);

		return Arrays.stream(components).anyMatch(Objects::isNull) ? null
			: RoutingKey.compose(components);
	}

	@Override
	public Token getRoutingToken() {
		return routingToken;
	}

	@Override
	@NonNull
	public Map<String, ByteBuffer> getCustomPayload() {
		return customPayload;
	}

	@Override
	public Boolean isIdempotent() {
		return idempotent;
	}

	@Override
	public Duration getTimeout() {
		return timeout;
	}

	@Override
	public Node getNode() {
		return node;
	}

}
