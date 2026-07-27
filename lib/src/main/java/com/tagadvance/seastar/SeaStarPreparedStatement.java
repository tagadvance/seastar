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
import java.util.stream.IntStream;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.NonNull;

/**
 * Safe for concurrent use: {@code definitions}, {@code resultMetadataId} and
 * {@code resultSetOverride} are {@code volatile}, resolved at most once under a
 * double-checked-locking {@code synchronized}, and every value ever published to them is
 * immutable once built.
 */
@ThreadSafe
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

	/**
	 * Resolves and caches the definitions, so that a statement this session rejects is rejected by
	 * {@code prepare()} itself.
	 */
	void primeDefinitions() {
		definitions();
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

	/**
	 * Resolves the statement's definitions, failing the way a live cluster fails a prepare that names
	 * something that does not exist. {@link SeaStarCqlPrepareHandler} calls this eagerly so the
	 * failure surfaces from {@code prepare()} rather than from a later
	 * {@link #getVariableDefinitions()}.
	 */
	BindMarkers.Definitions resolveDefinitions() {
		final var explicit = Optional.ofNullable(prepareRequest.getKeyspace()).orElse(keyspace);
		final var raw = CqlParsers.parse(context.getNode(), getQuery());

		return BindMarkers.resolve(context, explicit, raw);
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

	/**
	 * An opaque digest of the result-set definitions, as a buffer positioned to be read.
	 *
	 * <p>The same instance is returned on every call, which is what the driver does: a caller that
	 * consumes the buffer leaves it drained for the next one. Handing out a fresh duplicate would be
	 * friendlier but would let code pass here and fail against a cluster. Read-only, so the contents
	 * at least cannot be rewritten.
	 */
	@Override
	public ByteBuffer getResultMetadataId() {
		var current = resultMetadataId;
		if (current == null) {
			synchronized (this) {
				current = resultMetadataId;
				if (current == null) {
					final var hash = getResultSetDefinitions().hashCode();
					current = ByteBuffer.allocate(Integer.BYTES).putInt(hash).flip().asReadOnlyBuffer();
					resultMetadataId = current;
				}
			}
		}

		return current;
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

	/**
	 * Values are looked up in the codec registry as they are bound rather than when the statement
	 * runs, so a value the target column cannot hold fails with the
	 * {@link com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException} the real driver
	 * throws, at the point the real driver throws it. Trailing values may be omitted; those bind
	 * markers are simply left unset.
	 */
	@Override
	public @NonNull BoundStatement bind(final Object @NonNull ... values) {
		final var variables = getVariableDefinitions();
		if (variables.size() > 0 && values.length > variables.size()) {
			throw new IllegalArgumentException(
				"Too many variables (expected %d, got %d)".formatted(variables.size(), values.length));
		}

		final var codecRegistry = context.getCodecRegistry();
		IntStream.range(0, Math.min(values.length, variables.size()))
			.filter(i -> values[i] != null)
			.forEach(i -> codecRegistry.codecFor(variables.get(i).getType(), values[i]));

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
