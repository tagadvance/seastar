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
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tagadvance.seastar.SeaStarCqlSession;
import com.tagadvance.seastar.handlers.CqlStatementSummary;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

	private final SeaStarCqlSession session;
	private final PreparedStatements prepared = new PreparedStatements();

	SeaStarRequestDispatcher(final SeaStarCqlSession session) {
		this.session = requireNonNull(session, "session must not be null");
	}

	/**
	 * @param request    the decoded request frame, always at protocol v4
	 * @param connection the state of the connection it arrived on
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

		CqlStatementSummary summary = null;
		try {
			selectKeyspace(connection);
			summary = summarize(request.query);

			return answer(summary, run(statement(request.query, request.options)), connection);
		} catch (final RuntimeException e) {
			return Failures.of(e, summary);
		}
	}

	private Message prepare(final Prepare request, final SeaStarConnection connection) {
		try {
			selectKeyspace(connection);
			final var statement = session.prepare(request.cqlQuery);

			// resultMetadataId is a v5 field; the codec does not write it at v4, and a real node sends
			// null there, so nothing is lost by not inventing one.
			return new Prepared(prepared.register(statement), null,
				metadata(statement.getVariableDefinitions(), statement.getPartitionKeyIndices()),
				metadata(statement.getResultSetDefinitions(), List.of()));
		} catch (final RuntimeException e) {
			return Failures.of(e, null);
		}
	}

	private Message execute(final Execute request, final SeaStarConnection connection) {
		final var refusal = refusePagingState(request.options);
		if (refusal != null) {
			return refusal;
		}

		final var statement = prepared.find(request.queryId);
		if (statement.isEmpty()) {
			return unprepared(request.queryId);
		}

		CqlStatementSummary summary = null;
		try {
			selectKeyspace(connection);
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
			selectKeyspace(connection);
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
			return Results.of(run(builder.build()));
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
		final var response = Results.of(summary, resultSet);
		if (summary instanceof CqlStatementSummary.KeyspaceSelected selected) {
			connection.keyspace(CqlIdentifier.fromInternal(selected.keyspace()));
		}

		return response;
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
	 */
	private void selectKeyspace(final SeaStarConnection connection) {
		session.setKeyspace(connection.keyspace());
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
		if (!positionalValues.isEmpty() && positionalValues.size() != variables.size()) {
			throw new InvalidQueryException(node(), "Invalid amount of bind variables");
		}

		var bound = statement.bind();
		for (int i = 0; i < positionalValues.size(); i++) {
			bound = bound.setBytesUnsafe(i, positionalValues.get(i));
		}
		for (final var value : namedValues.entrySet()) {
			// firstIndexOf rather than setBytesUnsafe(String, ...): the driver's by-name default goes
			// through allIndicesOf, which SeaStar's bound statement does not override and which logs a
			// warning every time it is called.
			bound = bound.setBytesUnsafe(index(variables, value.getKey()), value.getValue());
		}

		return bound;
	}

	private int index(final ColumnDefinitions variables, final String name) {
		final var index = variables.firstIndexOf(name);
		if (index < 0) {
			throw new InvalidQueryException(node(),
				"Undefined name " + name + " in bind variables");
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
		final List<Integer> partitionKeyIndices) {
		final var pkIndices = partitionKeyIndices.stream().mapToInt(Integer::intValue).toArray();

		return definitions.size() == 0 ? new RowsMetadata(0, null, pkIndices, null)
			: new RowsMetadata(Results.specs(definitions), null, pkIndices, null);
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
