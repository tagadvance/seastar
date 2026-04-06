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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;

public class SeaStarBoundStatement implements BoundStatement {

	private final SeaStarDriverContext context;
	private final PreparedStatement preparedStatement;
	private final Object[] values;
	private final AtomicLong timestamp = new AtomicLong();

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
		// FIXME
//		ValuesHelper.encodeValues(values, type.getFieldTypes(),
//			type.getAttachmentPoint().getCodecRegistry(),
//			type.getAttachmentPoint().getProtocolVersion())

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
