package com.tagadvance.seastar.server;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import net.jcip.annotations.Immutable;
import org.jspecify.annotations.Nullable;

/**
 * A query against one of the keyspaces the server answers itself, recognized by its text.
 *
 * <p><strong>Matching a query string is deliberately grubby, and it is the right amount of
 * engineering for this.</strong> The queries that have to be answered here are the driver's own
 * control-connection queries; there are six of them, they are string literals in the driver's
 * source, and they do not vary. Parsing CQL properly to answer them would mean putting
 * {@code cassandra-all} on this module's classpath and teaching the core's handler registry about
 * tables that do not exist in the model - which is exactly what {@code d_plan D1} rules out, and
 * what a fake system keyspace in the model would cost every in-process user. Simulacron matches
 * strings for the same reason.
 *
 * <p>The literals, from java-driver-core 4.19.3:
 *
 * <pre>
 * ProtocolInitHandler        SELECT cluster_name FROM system.local
 * DefaultTopologyMonitor     SELECT * FROM system.local
 *                            SELECT * FROM system.peers_v2
 *                            SELECT * FROM system.peers
 * SchemaAgreementChecker     SELECT schema_version FROM system.local WHERE key='local'
 *                            SELECT * FROM system.peers
 * Cassandra3SchemaQueries    SELECT * FROM system_schema.&lt;table&gt;
 * </pre>
 *
 * <p>A {@code WHERE} clause is matched and then ignored. The only one a driver sends is
 * {@code key='local'}, and {@code system.local} has exactly one row, so restricting it would be a
 * predicate that is always true.
 *
 * @param keyspace the keyspace named by the query, always qualified - an unqualified query is not a
 *                 system query, whatever the connection's keyspace happens to be
 * @param table    the table named by the query
 * @param columns  the columns it selected, lower-cased, or empty for {@code SELECT *}
 */
@Immutable
record SystemQuery(String keyspace, String table, List<String> columns) {

	/**
	 * {@code SELECT <columns> FROM <keyspace>.<table> [WHERE ...][;]} and nothing else. Anything with
	 * a {@code GROUP BY}, an {@code ORDER BY}, a {@code LIMIT} or a function call in its selectors
	 * fails to match and falls through to the model, where it becomes an ordinary unknown-keyspace
	 * error.
	 */
	private static final Pattern SELECT = Pattern.compile(
		"\\s*SELECT\\s+(?<columns>[^()]+?)\\s+FROM\\s+(?<keyspace>\\w+)\\s*\\.\\s*(?<table>\\w+)"
			+ "\\s*(?:WHERE\\s.*?)?;?\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	/**
	 * @param query the query string as it arrived
	 * @return the query, or {@code null} if it is not a select against a qualified table - which is
	 * every statement the model should answer instead
	 */
	static @Nullable SystemQuery of(final String query) {
		final var matcher = SELECT.matcher(query);
		if (!matcher.matches()) {
			return null;
		}

		final var columns = matcher.group("columns").trim();

		return new SystemQuery(identifier(matcher.group("keyspace")),
			identifier(matcher.group("table")),
			"*".equals(columns) ? List.of() : List.of(columns.split(",")).stream()
				.map(SystemQuery::identifier)
				.toList());
	}

	/**
	 * Narrows a full table to the columns this query asked for.
	 *
	 * <p>It matters for two of the driver's queries and for no other reason: {@code ProtocolInitHandler}
	 * reads the cluster name <em>positionally</em>, out of column 0, so handing it the whole of
	 * {@code system.local} would have it read {@code key} and believe the cluster is called "local".
	 *
	 * @param message the full table, as {@code ROWS}
	 * @return the same message narrowed to {@link #columns}, unchanged for {@code SELECT *}, or an
	 * {@code INVALID} error naming the first column the table does not have
	 */
	Message project(final Message message) {
		if (columns.isEmpty() || !(message instanceof Rows rows)) {
			return message;
		}

		final var available = rows.getMetadata().columnSpecs;
		final var indices = new ArrayList<Integer>(columns.size());
		final var specs = new ArrayList<ColumnSpec>(columns.size());
		for (final var column : columns) {
			final var index = indexOf(available, column);
			if (index < 0) {
				return new Error(ProtocolConstants.ErrorCode.INVALID,
					"Undefined column name " + column);
			}
			final var spec = available.get(index);
			indices.add(index);
			specs.add(new ColumnSpec(spec.ksName, spec.tableName, spec.name, specs.size(), spec.type));
		}

		final var data = new ArrayDeque<List<ByteBuffer>>(rows.getData().size());
		for (final var row : rows.getData()) {
			final var values = new ArrayList<ByteBuffer>(indices.size());
			indices.forEach(index -> values.add(row.get(index)));
			data.add(values);
		}

		return new DefaultRows(new RowsMetadata(specs, null, null, null), data);
	}

	private static int indexOf(final List<ColumnSpec> specs, final String name) {
		for (int i = 0; i < specs.size(); i++) {
			if (specs.get(i).name.equals(name)) {
				return i;
			}
		}

		return -1;
	}

	/**
	 * An identifier as CQL reads it: case-folded to lower unless it was quoted. The driver never
	 * quotes one of these, so this exists only so that a hand-written {@code SELECT "key" FROM ...}
	 * behaves the way the same query does against a node.
	 */
	private static String identifier(final String name) {
		final var trimmed = name.trim();

		return trimmed.length() > 1 && trimmed.charAt(0) == '"' && trimmed.endsWith("\"")
			? trimmed.substring(1, trimmed.length() - 1) : trimmed.toLowerCase(Locale.ROOT);
	}

}
