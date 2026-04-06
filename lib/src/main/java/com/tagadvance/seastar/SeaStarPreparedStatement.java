package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.jspecify.annotations.NonNull;

public class SeaStarPreparedStatement implements PreparedStatement {

	private final SeaStarDriverContext context;
	private final ByteBuffer id;
	private final PrepareRequest prepareRequest;

	public SeaStarPreparedStatement(final SeaStarDriverContext context,
		final SimpleStatement statement) {
		this(context, new DefaultPrepareRequest(statement));
	}

	public SeaStarPreparedStatement(final SeaStarDriverContext context, final String query) {
		this(context, new DefaultPrepareRequest(query));
	}

	protected SeaStarPreparedStatement(final SeaStarDriverContext context,
		final PrepareRequest request) {
		this.context = requireNonNull(context, "context must not be null");
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
		return new SeaStarBoundStatement(context, this);
	}

	@Override
	@NonNull
	public BoundStatementBuilder boundStatementBuilder(final Object @NonNull ... values) {
		// FIXME
		throw new UnsupportedOperationException();
	}

}
