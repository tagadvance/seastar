package com.tagadvance.seastar.server;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.SystemSchema;
import com.tagadvance.seastar.handlers.CqlStatementSummary;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;
import org.jspecify.annotations.Nullable;

/**
 * Where the wire meets the model: a decoded {@code QUERY}, {@code PREPARE}, {@code EXECUTE} or
 * {@code BATCH} arrives, and a response message has to come back.
 *
 * <p>Two things hold for every call, and the rest of the server exists to make them hold:
 *
 * <ul>
 *   <li><strong>It runs on the funnel</strong> - the server's single-threaded executor, never a
 *       Netty event loop - so no two requests are ever in the session at once, whichever
 *       connection they arrived on. That is what makes it safe to point the session at the
 *       connection's keyspace immediately before running a statement.</li>
 *   <li><strong>A thrown exception is caught by the caller</strong>
 *       ({@link SeaStarProtocolHandler}) and turned into a {@code SERVER_ERROR} carrying its
 *       message. That is the last resort, not the channel: every failure that has a more specific
 *       code goes back as an {@link Error} through {@link Failures}, because the whole point is
 *       that a client rebuilds the same exception the in-process caller would have caught.</li>
 * </ul>
 *
 * <p>Handshake messages do not come through here. {@code OPTIONS}, {@code STARTUP},
 * {@code REGISTER} and {@code AUTH_RESPONSE} are transport, and the handler answers them itself.
 *
 * <p>Accepted and ignored, matching what the in-process builder already does with the same
 * settings: consistency, serial consistency, the default timestamp, {@code now_in_seconds} and the
 * page size. SeaStar has one replica, answers from memory and never pages, so none of them has
 * anything to change. A paging state is different - see {@link #refusePagingState}.
 */
@ThreadSafe
final class SeaStarRequestDispatcher {

	/**
	 * What a node answers when a request's values do not account for exactly the bind markers its
	 * statement carries, whichever side the discrepancy is on.
	 */
	private static final String WRONG_VALUE_COUNT = "Invalid amount of bind variables";

	private final SeaStarCqlSession session;
	private final SystemTables systemTables;
	private final Collection<SeaStarConnection> connections;
	private final PreparedStatements prepared = new PreparedStatements();

	/**
	 * The system queries this server has prepared, by id. Separate from {@link #prepared} because a
	 * system query never reaches the session, so there is no {@link PreparedStatement} to remember -
	 * only the text to answer again.
	 */
	private final Map<ByteBuffer, String> preparedSystem = new ConcurrentHashMap<>();

	SeaStarRequestDispatcher(final SeaStarCqlSession session, final SystemTables systemTables,
		final Collection<SeaStarConnection> connections) {
		this.session = requireNonNull(session, "session must not be null");
		this.systemTables = requireNonNull(systemTables, "systemTables must not be null");
		this.connections = requireNonNull(connections, "connections must not be null");
	}

	/**
	 * @param request    the decoded request frame
	 * @param connection the state of the connection it arrived on, which is where the protocol
	 *                   version every answer is described with comes from
	 * @return the message to send back, on the request's own stream id
	 */
	Message dispatch(final Frame request, final SeaStarConnection connection) {
		final var message = request.message;
		if (message instanceof Query query) {
			return query(query, connection);
		}
		if (message instanceof Prepare prepare) {
			return prepare(prepare, connection);
		}
		if (message instanceof Execute execute) {
			return execute(execute, connection);
		}
		if (message instanceof Batch batch) {
			return batch(batch, connection);
		}

		return new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
			"Unsupported request opcode: " + message.opcode);
	}

	private Message query(final Query request, final SeaStarConnection connection) {
		final var refusal = refusePagingState(request.options);
		if (refusal != null) {
			return refusal;
		}

		final var system = system(request.query, connection.version());
		if (system != null) {
			return system;
		}

		CqlStatementSummary summary = null;
		try {
			selectKeyspace(connection, request.options.keyspace);
			summary = summarize(request.query);

			return answer(summary, run(statement(request.query, request.options)), connection);
		} catch (final RuntimeException e) {
			return Failures.of(e, summary);
		}
	}

	private Message prepare(final Prepare request, final SeaStarConnection connection) {
		final var version = connection.version();
		final var select = SystemQuery.of(request.cqlQuery);
		if (select != null && isSystem(select)) {
			return prepareSystem(request.cqlQuery, version);
		}

		try {
			selectKeyspace(connection, request.keyspace);
			final var statement = session.prepare(request.cqlQuery);

			return new Prepared(prepared.register(statement), resultMetadataId(statement, version),
				metadata(statement.getVariableDefinitions(), statement.getPartitionKeyIndices(),
					version),
				metadata(statement.getResultSetDefinitions(), List.of(), version));
		} catch (final RuntimeException e) {
			return Failures.of(e, null);
		}
	}

	/**
	 * The identifier of a prepared statement's <em>result</em> metadata, which the protocol only
	 * carries from v5 on - the v4 codec does not write the field, and a real node reached at v4
	 * sends null.
	 *
	 * <p>Duplicated on the way out. The core hands back the same buffer instance every time, exactly
	 * as the driver's own {@code DefaultPreparedStatement} does, and encoding a frame reads a buffer
	 * to its limit and leaves the position there - so preparing the same statement twice would send
	 * an empty id the second time. The duplicate belongs here, where the buffer is written, rather
	 * than in the core, where the shared instance is the behaviour a caller has to be able to rely
	 * on (e_plan E4).
	 */
	private static byte @Nullable [] resultMetadataId(final PreparedStatement statement,
		final int version) {
		if (version < ProtocolConstants.Version.V5) {
			return null;
		}

		return Bytes.getArray(statement.getResultMetadataId().duplicate());
	}

	private Message execute(final Execute request, final SeaStarConnection connection) {
		final var refusal = refusePagingState(request.options);
		if (refusal != null) {
			return refusal;
		}

		final var systemQuery = preparedSystem.get(ByteBuffer.wrap(request.queryId));
		if (systemQuery != null) {
			return system(systemQuery, connection.version());
		}

		final var statement = prepared.find(request.queryId);
		if (statement.isEmpty()) {
			return unprepared(request.queryId);
		}

		CqlStatementSummary summary = null;
		try {
			selectKeyspace(connection, request.options.keyspace);
			summary = summarize(statement.get().getQuery());
			final var bound = bind(statement.get(), request.options.positionalValues,
				request.options.namedValues);

			return answer(summary, run(bound), connection);
		} catch (final RuntimeException e) {
			return Failures.of(e, summary);
		}
	}

	private Message batch(final Batch request, final SeaStarConnection connection) {
		try {
			selectKeyspace(connection, request.keyspace);
			final var builder = BatchStatement.builder(batchType(request.type));
			for (int i = 0; i < request.queriesOrIds.size(); i++) {
				final var queryOrId = request.queriesOrIds.get(i);
				final var values = request.values.get(i);
				if (queryOrId instanceof String cql) {
					builder.addStatement(values.isEmpty() ? SimpleStatement.newInstance(cql)
						: bind(session.prepare(cql), values, Map.of()));
				} else {
					final var id = (byte[]) queryOrId;
					final var statement = prepared.find(id);
					if (statement.isEmpty()) {
						return unprepared(id);
					}
					builder.addStatement(bind(statement.get(), values, Map.of()));
				}
			}

			// A batch is modifications only - the core rejects anything else in one - so it never
			// selects a keyspace and never changes the schema.
			return Results.of(run(builder.build()), connection.version());
		} catch (final RuntimeException e) {
			return Failures.of(e, null);
		}
	}

	/**
	 * Builds the response and, for a {@code USE} that worked, records the keyspace against the
	 * connection. The order matters: the connection is only moved once the statement has run, so a
	 * {@code USE} naming a keyspace that does not exist leaves it where it was.
	 */
	private Message answer(final CqlStatementSummary summary, final AsyncResultSet resultSet,
		final SeaStarConnection connection) {
		final var response = Results.of(summary, resultSet, connection.version());
		if (summary instanceof CqlStatementSummary.KeyspaceSelected selected) {
			connection.keyspace(CqlIdentifier.fromInternal(selected.keyspace()));
		}
		if (summary instanceof CqlStatementSummary.SchemaChanged changed) {
			// Before the response goes out, so that the schema-agreement check the driver runs on
			// seeing it already reads the new version.
			systemTables.schemaChanged();
			publish(Results.event(changed));
		}

		return response;
	}

	/**
	 * Tells every connection that registered for one that the schema moved (f_plan F2).
	 *
	 * <p>The result of a DDL statement goes back to the connection that ran it and nowhere else, so
	 * without this a second client watching the same server never learns that anything happened -
	 * which is exactly what a driver registers for {@code SCHEMA_CHANGE} to find out.
	 *
	 * <p>An event may be sent at any time, including between a request and its response, so no
	 * attempt is made to order this against the result of the statement that caused it. On the
	 * connection that ran the DDL both arrive; the protocol requires neither to be first, and a
	 * client routes them apart by stream id rather than by order.
	 *
	 * @param event the event to publish, or {@code null} if the statement changed nothing after all
	 */
	private void publish(final @Nullable Event event) {
		if (event == null) {
			return;
		}
		connections.forEach(connection -> connection.publish(event));
	}

	/**
	 * Answers the keyspaces a real node keeps to describe itself, which the model has no idea about:
	 * {@code system} from {@link SystemTables}, {@code system_schema} from the core's projection.
	 *
	 * <p>This is the whole of {@code d_plan D1}, and it is on purpose that it happens here rather
	 * than in the core's handler registry. These tables are a property of the listener, not of the
	 * data: an in-process user who never starts a server must not suddenly find invented keyspaces in
	 * {@code getMetadata().getKeyspaces()}.
	 *
	 * @param query           the query string, exactly as it arrived
	 * @param protocolVersion the version of the connection it is going out on
	 * @return the answer, or {@code null} if this is not a query the server answers itself - which is
	 * everything the model should see
	 */
	private @Nullable Message system(final String query, final int protocolVersion) {
		final var select = SystemQuery.of(query);
		if (select == null || !isSystem(select)) {
			return null;
		}
		if (SystemTables.answers(select.keyspace())) {
			return systemTables.select(select, protocolVersion);
		}

		return SystemSchema.select(session.getContext(), select.table())
			.map(resultSet -> Results.of(resultSet, protocolVersion))
			.map(select::project)
			.orElseGet(() -> unconfiguredTable(select.table()));
	}

	/**
	 * Answers a {@code PREPARE} of a system query without going near the session, which has no such
	 * tables to resolve the statement against.
	 *
	 * <p>The id is derived from the query text, so preparing the same string twice returns the same
	 * id - which is what a real node does. There are no bind variables: none of these queries has
	 * one, and the {@code WHERE} clause that could have carried one is ignored anyway.
	 */
	private Message prepareSystem(final String query, final int protocolVersion) {
		final var answer = system(query, protocolVersion);
		if (!(answer instanceof Rows rows)) {
			// The table does not exist, and that is the same error an EXECUTE of it would give.
			return answer;
		}

		final var uuid = UUID.nameUUIDFromBytes(query.getBytes(StandardCharsets.UTF_8));
		final var id = ByteBuffer.allocate(2 * Long.BYTES)
			.putLong(uuid.getMostSignificantBits())
			.putLong(uuid.getLeastSignificantBits())
			.array();
		preparedSystem.put(ByteBuffer.wrap(id), query);

		// The result metadata of a system query never changes, so its identifier can be the query's
		// own id. At v4 the field is not written at all.
		final var resultMetadataId = protocolVersion < ProtocolConstants.Version.V5 ? null : id;

		return new Prepared(id, resultMetadataId, new RowsMetadata(0, null, null, null),
			rows.getMetadata());
	}

	private static boolean isSystem(final SystemQuery select) {
		return SystemTables.answers(select.keyspace())
			|| SystemSchema.KEYSPACE_NAME.equals(select.keyspace());
	}

	private static Message unconfiguredTable(final String table) {
		return new Error(ProtocolConstants.ErrorCode.INVALID, "table " + table + " does not exist");
	}

	/**
	 * Points the session at the keyspace this connection selected, which may be none.
	 *
	 * <p>A real node keeps the selected keyspace per connection and a driver opens several; SeaStar
	 * keeps one per session. They are reconciled here, on the funnel, immediately before the
	 * statement runs - which is only safe because the funnel is the single thread that ever touches
	 * the session.
	 *
	 * <p>Set rather than {@code USE}d, and the difference is visible from a client. A connection is
	 * remembering a <em>name</em>, not a keyspace: setting it accepts one that does not exist, so
	 * dropping the selected keyspace leaves unqualified statements failing with "keyspace x does not
	 * exist" while qualified ones elsewhere keep working, and recreating it makes the connection work
	 * again. All three were taken off a {@code cassandra:5.0.8} container. A {@code USE} would have
	 * failed the whole statement instead, and could not express "no keyspace" at all - which is what
	 * a connection that never ran {@code USE} has to have while another connection has one.
	 *
	 * <p>Protocol v5 lets a single request name its own keyspace, which wins for that statement and
	 * leaves the connection where it was. A v4 request cannot carry one - the driver refuses to send
	 * it - so the argument is always null there.
	 *
	 * @param connection    the connection the request arrived on
	 * @param perRequest    the keyspace this one request named, or {@code null} for the
	 *                      connection's own
	 */
	private void selectKeyspace(final SeaStarConnection connection,
		final @Nullable String perRequest) {
		session.setKeyspace(
			perRequest == null ? connection.keyspace() : CqlIdentifier.fromInternal(perRequest));
	}

	private CqlStatementSummary summarize(final String query) {
		// Before the statement runs, not after: DROP INDEX names only the index, so the table a
		// driver has to be told about can only be found while the index is still there.
		return CqlStatementSummary.of(session.getMetadata(), session.getKeyspace().orElse(null),
			query);
	}

	private Statement<?> statement(final String query, final QueryOptions options) {
		if (options.positionalValues.isEmpty() && options.namedValues.isEmpty()) {
			return SimpleStatement.newInstance(query);
		}

		// The values arrive as raw buffers with no type information, because a server is supposed to
		// know the types from its schema. Preparing is how those types are resolved, and the bound
		// statement it produces is the shape the core already reads bound values out of - so nothing
		// is decoded and re-encoded on the way through.
		return bind(session.prepare(query), options.positionalValues, options.namedValues);
	}

	private BoundStatement bind(final PreparedStatement statement,
		final List<ByteBuffer> positionalValues, final Map<String, ByteBuffer> namedValues) {
		final var variables = statement.getVariableDefinitions();
		// A node counts the values against the markers before it reads any of them, and a name that is
		// no marker of this statement leaves one of them unaccounted for, so it reports the same thing.
		final var supplied = positionalValues.isEmpty() ? namedValues.size() : positionalValues.size();
		if (supplied != variables.size()) {
			throw new InvalidQueryException(node(), WRONG_VALUE_COUNT);
		}

		var bound = statement.bind();
		try {
			for (int i = 0; i < positionalValues.size(); i++) {
				bound = bound.setBytesUnsafe(i, positionalValues.get(i));
			}
			for (final var value : namedValues.entrySet()) {
				// firstIndexOf rather than setBytesUnsafe(String, ...): the driver's by-name default goes
				// through allIndicesOf, which SeaStar's bound statement does not override and which logs a
				// warning every time it is called.
				bound = bound.setBytesUnsafe(index(variables, value.getKey()), value.getValue());
			}
		} catch (final IllegalArgumentException e) {
			// A buffer the column's type cannot decode is an IllegalArgumentException from the driver's
			// own codec, which would travel as a SERVER_ERROR. A node reports what the client sent as
			// invalid, so the message is kept and only the code changes.
			throw new InvalidQueryException(node(), e.getMessage());
		}

		return bound;
	}

	private int index(final ColumnDefinitions variables, final String name) {
		final var index = variables.firstIndexOf(name);
		if (index < 0) {
			throw new InvalidQueryException(node(), WRONG_VALUE_COUNT);
		}

		return index;
	}

	/**
	 * Runs a statement and hands back its result set.
	 *
	 * <p>{@code join} never blocks: the core answers every request on the calling thread, so the
	 * stage is already complete by the time it returns. It is used rather than the synchronous API
	 * because {@link AsyncResultSet} is also what the system-table answers are shaped as.
	 */
	private AsyncResultSet run(final Statement<?> statement) {
		return session.executeAsync(statement).toCompletableFuture().join();
	}

	/**
	 * A paging state can only have come from this server, and this server never issues one, so one
	 * arriving is a client that has confused us with something else.
	 *
	 * <p>Refused rather than ignored, and the difference matters: ignoring it would answer with page
	 * one again, and a client that asked for page two and got page one asks for page two again. The
	 * code and the wording are what {@code cassandra:5.0.8} sends for a paging state it cannot read.
	 *
	 * @param options the options of the request
	 * @return the error to answer with, or {@code null} if there was no paging state
	 */
	private static @Nullable Message refusePagingState(final QueryOptions options) {
		if (options.pagingState == null) {
			return null;
		}

		return new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
			"Invalid value for the paging state");
	}

	private static Message unprepared(final byte[] id) {
		return new Unprepared("Prepared query with ID " + Bytes.toHexString(id)
			+ " not found (either the query was not prepared on this host, or it was prepared on a "
			+ "server that has since been restarted)", id);
	}

	/**
	 * Column definitions as the protocol's rows metadata. A statement with nothing to return -
	 * an {@code INSERT}'s result metadata, for instance - is described as having no metadata at all
	 * rather than as an empty column list, which is what a real node sends.
	 */
	private static RowsMetadata metadata(final ColumnDefinitions definitions,
		final List<Integer> partitionKeyIndices, final int protocolVersion) {
		final var pkIndices = partitionKeyIndices.stream().mapToInt(Integer::intValue).toArray();

		return definitions.size() == 0 ? new RowsMetadata(0, null, pkIndices, null)
			: new RowsMetadata(Results.specs(definitions, protocolVersion), null, pkIndices, null);
	}

	private static DefaultBatchType batchType(final byte type) {
		return switch (type) {
			case ProtocolConstants.BatchType.UNLOGGED -> DefaultBatchType.UNLOGGED;
			case ProtocolConstants.BatchType.COUNTER -> DefaultBatchType.COUNTER;
			default -> DefaultBatchType.LOGGED;
		};
	}

	private Node node() {
		return session.getMetadata().getNodes().values().stream().findFirst().orElse(null);
	}

}
