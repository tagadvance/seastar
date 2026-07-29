package com.tagadvance.seastar.server;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.tagadvance.seastar.handlers.CqlStatementSummary;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * A SeaStar answer as the protocol's four result messages: {@code ROWS}, {@code VOID},
 * {@code SET_KEYSPACE} and {@code SCHEMA_CHANGE}.
 *
 * <p>Which one a statement gets is almost entirely a question for its result set - a result set
 * with columns is {@code ROWS} and one without is {@code VOID}, which is right for {@code SELECT},
 * for the modifications, for {@code TRUNCATE}, for {@code BATCH}, and for a lightweight transaction
 * without any special casing, since {@code [applied]} is a column like any other. The two
 * statements that break that rule are {@code USE} and DDL, and a {@link CqlStatementSummary} is how
 * the core says which of those it was.
 *
 * <p><strong>Paging is not implemented, and that is protocol-legal.</strong> Rows metadata with no
 * paging state means "this is the last page", so a driver reads the answer as complete and stops
 * asking. A node is always free to return everything; what it is not free to do is accept a paging
 * state it never issued, which is why one arriving in a request is refused rather than ignored (see
 * {@link SeaStarRequestDispatcher}).
 */
final class Results {

	private Results() {

	}

	/**
	 * @param summary         what the statement was, as the core summarized it before it ran
	 * @param resultSet       what it answered with
	 * @param protocolVersion the version of the connection it is going out on
	 * @return the result message to send back
	 */
	static Message of(final CqlStatementSummary summary, final AsyncResultSet resultSet,
		final int protocolVersion) {
		if (summary instanceof CqlStatementSummary.KeyspaceSelected selected) {
			return new SetKeyspace(selected.keyspace());
		}
		if (summary instanceof CqlStatementSummary.SchemaChanged changed) {
			final var change = schemaChange(changed);

			return change == null ? Void.INSTANCE : change;
		}

		return of(resultSet, protocolVersion);
	}

	/**
	 * The same schema change again, as the event pushed to every connection that registered for
	 * one. The driver expects both: the result tells the connection that ran the DDL, the event
	 * tells every other client watching.
	 *
	 * @param changed what the statement changed, as the core summarized it before it ran
	 * @return the event to publish, or {@code null} if nothing actually changed
	 */
	static @Nullable SchemaChangeEvent event(final CqlStatementSummary.SchemaChanged changed) {
		final var change = schemaChange(changed);

		return change == null ? null : new SchemaChangeEvent(change.changeType, change.target,
			change.keyspace, change.object, change.arguments);
	}

	/**
	 * The result set on its own, for a caller that already knows the statement answers with rows or
	 * with nothing - a prepared {@code EXECUTE}, or one of the system tables the server answers
	 * itself.
	 *
	 * @param resultSet       what the statement answered with
	 * @param protocolVersion the version of the connection it is going out on
	 * @return {@code ROWS} if it has any columns, {@code VOID} otherwise
	 */
	static Message of(final AsyncResultSet resultSet, final int protocolVersion) {
		final var definitions = resultSet.getColumnDefinitions();

		return definitions.size() == 0 ? Void.INSTANCE
			: rows(definitions, resultSet, protocolVersion);
	}

	/**
	 * Column definitions as the protocol writes them, for a result set or for the two halves of a
	 * {@code PREPARED}.
	 *
	 * @param definitions     the columns to describe
	 * @param protocolVersion the version of the connection they are going out on
	 * @return one spec per column, in order
	 */
	static List<ColumnSpec> specs(final ColumnDefinitions definitions, final int protocolVersion) {
		final var specs = new ArrayList<ColumnSpec>(definitions.size());
		for (int i = 0; i < definitions.size(); i++) {
			final var definition = definitions.get(i);
			specs.add(new ColumnSpec(name(definition.getKeyspace()), name(definition.getTable()),
				definition.getName().asInternal(), i,
				RawTypes.of(definition.getType(), protocolVersion)));
		}

		return specs;
	}

	private static Message rows(final ColumnDefinitions definitions,
		final AsyncResultSet resultSet, final int protocolVersion) {
		final var data = new ArrayDeque<List<ByteBuffer>>(resultSet.remaining());
		for (final var row : resultSet.currentPage()) {
			final var values = new ArrayList<ByteBuffer>(definitions.size());
			for (int i = 0; i < definitions.size(); i++) {
				final var bytes = row.getBytesUnsafe(i);
				// Duplicated because encoding a frame reads to the buffer's limit and leaves its
				// position there. Handing the same instance out twice would answer the second read with
				// nothing, which presents as "the first query works and the rest come back null".
				values.add(bytes == null ? null : bytes.duplicate());
			}
			data.add(values);
		}

		// No paging state: there is no next page, ever. RowsMetadata computes its own flags from the
		// specs, which is what sets GLOBAL_TABLES_SPEC when every column shares a table.
		return new DefaultRows(
			new RowsMetadata(specs(definitions, protocolVersion), null, null, null), data);
	}

	/**
	 * @return the change, or {@code null} if the statement turned out to change nothing - which a
	 *     node answers {@code VOID} and tells nobody about
	 */
	private static @Nullable SchemaChange schemaChange(
		final CqlStatementSummary.SchemaChanged changed) {
		if (changed.target() != CqlStatementSummary.Target.KEYSPACE && changed.object() == null) {
			// Only DROP INDEX can get here, and only when the index was not found - which means an
			// IF EXISTS that did nothing, since anything else has already failed. There is no table to
			// name and nothing changed, so VOID is both what can be said and what a node would send.
			return null;
		}

		final var target = switch (changed.target()) {
			case KEYSPACE -> ProtocolConstants.SchemaChangeTarget.KEYSPACE;
			case TABLE -> ProtocolConstants.SchemaChangeTarget.TABLE;
			case TYPE -> ProtocolConstants.SchemaChangeTarget.TYPE;
		};
		final var change = switch (changed.change()) {
			case CREATED -> ProtocolConstants.SchemaChangeType.CREATED;
			case UPDATED -> ProtocolConstants.SchemaChangeType.UPDATED;
			case DROPPED -> ProtocolConstants.SchemaChangeType.DROPPED;
		};

		// The argument list is for functions and aggregates, which SeaStar does not implement.
		return new SchemaChange(change, target, changed.keyspace(), changed.object(), List.of());
	}

	private static String name(final @Nullable CqlIdentifier identifier) {
		return identifier == null ? "" : identifier.asInternal();
	}

}
