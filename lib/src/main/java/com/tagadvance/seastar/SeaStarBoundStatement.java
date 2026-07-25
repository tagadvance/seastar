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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.jspecify.annotations.NonNull;

public class SeaStarBoundStatement implements BoundStatement {

	private final SeaStarDriverContext context;
	private final PreparedStatement preparedStatement;
	private Object[] values;
	private final AtomicLong timestamp = new AtomicLong();
	private CqlIdentifier routingKeyspace;
	private Boolean idempotent;
	private Map<String, ByteBuffer> customPayload = Map.of();

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
		this.routingKeyspace = newRoutingKeyspace;

		return this;
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
		throw new UnsupportedOperationException();
	}

	@Override
	public DriverExecutionProfile getExecutionProfile() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CqlIdentifier getRoutingKeyspace() {
		return routingKeyspace;
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
		return customPayload;
	}

	@Override
	public Boolean isIdempotent() {
		return idempotent;
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
