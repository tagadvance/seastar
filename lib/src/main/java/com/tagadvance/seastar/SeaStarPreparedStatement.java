package com.tagadvance.seastar;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import com.tagadvance.seastar.handlers.BindMarkers;
import com.tagadvance.seastar.handlers.CqlParsers;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.NonNull;

public class SeaStarPreparedStatement implements PreparedStatement {

	private final SeaStarDriverContext context;
	private final ByteBuffer id;
	private final PrepareRequest prepareRequest;
	private final CqlIdentifier keyspace;

	private volatile BindMarkers.Definitions definitions;
	private volatile ByteBuffer resultMetadataId;
	private volatile ColumnDefinitions resultSetOverride;

	public SeaStarPreparedStatement(final SeaStarDriverContext context,
		final SimpleStatement statement) {
		this(context, new DefaultPrepareRequest(statement));
	}

	public SeaStarPreparedStatement(final SeaStarDriverContext context, final String query) {
		this(context, new DefaultPrepareRequest(query));
	}

	protected SeaStarPreparedStatement(final SeaStarDriverContext context,
		final PrepareRequest request) {
		this(context, request, null);
	}

	protected SeaStarPreparedStatement(final SeaStarDriverContext context,
		final PrepareRequest request, final CqlIdentifier keyspace) {
		this.context = requireNonNull(context, "context must not be null");
		this.id = ByteBuffer.wrap(UUID.randomUUID().toString().getBytes());
		this.prepareRequest = requireNonNull(request, "request must not be null");
		this.keyspace = keyspace;
	}

	private BindMarkers.Definitions definitions() {
		var local = definitions;
		if (local == null) {
			synchronized (this) {
				local = definitions;
				if (local == null) {
					local = resolveDefinitions();
					definitions = local;
				}
			}
		}

		return local;
	}

	private BindMarkers.Definitions resolveDefinitions() {
		final var explicit = Optional.ofNullable(prepareRequest.getKeyspace()).orElse(keyspace);
		try {
			final var raw = CqlParsers.parse(context.getNode(), getQuery());

			return BindMarkers.resolve(context, explicit, raw);
		} catch (final RuntimeException e) {
			return new BindMarkers.Definitions(EmptyColumnDefinitions.INSTANCE,
				EmptyColumnDefinitions.INSTANCE, List.of());
		}
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
		return definitions().variables();
	}

	@Override
	@NonNull
	public List<Integer> getPartitionKeyIndices() {
		return definitions().partitionKeyIndices();
	}

	@Override
	public ByteBuffer getResultMetadataId() {
		final var current = resultMetadataId;
		if (current != null) {
			return current;
		}

		final var hash = getResultSetDefinitions().hashCode();

		return ByteBuffer.allocate(4).putInt(hash);
	}

	@Override
	@NonNull
	public ColumnDefinitions getResultSetDefinitions() {
		final var override = resultSetOverride;

		return override != null ? override : definitions().resultSet();
	}

	@Override
	public void setResultMetadata(final @NonNull ByteBuffer newResultMetadataId,
		final @NonNull ColumnDefinitions newResultSetDefinitions) {
		this.resultMetadataId = requireNonNull(newResultMetadataId,
			"newResultMetadataId must not be null");
		this.resultSetOverride = requireNonNull(newResultSetDefinitions,
			"newResultSetDefinitions must not be null");
	}

	@Override
	public @NonNull BoundStatement bind(final Object @NonNull ... values) {
		final var variables = getVariableDefinitions();
		if (variables.size() > 0 && values.length > variables.size()) {
			throw new IllegalArgumentException(
				"Too many variables (expected %d, got %d)".formatted(variables.size(), values.length));
		}

		return new SeaStarBoundStatement(context, this, values);
	}

	@Override
	@NonNull
	public BoundStatementBuilder boundStatementBuilder(final Object @NonNull ... values) {
		final var variables = getVariableDefinitions();
		final var bound = (SeaStarBoundStatement) bind(values);
		final var encoded = bound.getValues().toArray(new ByteBuffer[variables.size()]);

		return new BoundStatementBuilder(this, variables, encoded, null, null, null, null, null,
			Map.of(), null, false, Statement.NO_DEFAULT_TIMESTAMP, null, Integer.MIN_VALUE, null, null,
			null, context.getCodecRegistry(), context.getProtocolVersion());
	}

}
