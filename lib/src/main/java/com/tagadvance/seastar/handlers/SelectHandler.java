package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.tagadvance.seastar.SeaStarAsyncResultSet;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.SelectStatement.RawStatement;
import org.jspecify.annotations.NonNull;

@ThreadSafe
public class SelectHandler implements CqlHandler<RawStatement> {

	private final Supplier<Optional<CqlIdentifier>> getKeyspace;

	public SelectHandler(final Supplier<Optional<CqlIdentifier>> getKeyspace) {
		this.getKeyspace = requireNonNull(getKeyspace, "getKeyspace must not be null");
	}

	@Override
	public boolean canProcess(final CQLStatement.Raw raw) {
		return raw instanceof RawStatement;
	}

	@Override
	public CompletionStage<AsyncResultSet> processCql(final SeaStarDriverContext context,
		final ExecutionInfo executionInfo, final RawStatement raw, final Object... bindings) {
		final var coordinator = executionInfo.getCoordinator();

		final Query query;
		final Projection projection;
		final Predicate<SeaStarRow> predicate;
		final int[] distinctKey;
		final RowOrdering ordering;
		try {
			query = Queries.translate(context, getKeyspace, raw, coordinator, bindings);
			projection = projection(query);
			if (query.distinct()) {
				validateDistinct(query, projection, coordinator);
			}
			predicate = resolveWhere(query, coordinator);
			distinctKey = query.distinct() ? partitionKeyIndices(query.target()) : null;
			ordering = RowOrdering.of(query.target().table(), false);
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var table = query.target().table();
		final var limit = query.limit();

		return table.readLockUnchecked(() -> {
			var rows = table.rows();
			if (predicate != null) {
				rows = rows.filter(predicate);
			}
			if (distinctKey != null) {
				// One row per partition. DISTINCT is validated to select only partition-key and
				// static columns, so the partition key fully identifies each distinct result.
				final Set<List<Object>> seen = new HashSet<>();
				rows = rows.filter(row -> seen.add(partitionKeyValues(row, distinctKey)));
			}
			// Ordering comes before the limit: LIMIT takes the first n rows of the result Cassandra
			// would return, not the first n rows that happen to have been inserted.
			rows = ordering.sort(rows);
			if (limit != null) {
				rows = rows.limit(limit);
			}
			final Stream<Row> snapshots = rows.map(SeaStarRow::snapshot);
			final Stream<Row> projected = projection == null ? snapshots
				: snapshots.map(row -> project(row, projection));
			final var data = projected.collect(Collectors.toCollection(LinkedList::new));
			final var definitions = projection == null ? table.snapshot() : projection.definitions();

			return CompletableFuture.<AsyncResultSet>completedStage(
				new SeaStarAsyncResultSet(definitions, executionInfo, data));
		});
	}

	private record Projection(ColumnDefinitions definitions, int[] indices) {

	}

	/**
	 * The definitions and column positions a projection reads, or null for {@code SELECT *}, which
	 * returns the table's own definitions.
	 */
	private static Projection projection(final Query query) {
		final var indices = query.projection();
		if (indices.isEmpty()) {
			return null;
		}

		final var table = query.target().table();
		final List<ColumnDefinition> columns = indices.stream().map(table::get).toList();

		return new Projection(DefaultColumnDefinitions.valueOf(columns),
			indices.stream().mapToInt(Integer::intValue).toArray());
	}

	private static void validateDistinct(final Query query, final Projection projection,
		final Node coordinator) {
		final var table = query.target().table();
		final var partitionKey = query.target().partitionKeyNames();
		// A null projection means SELECT *, which requests every column; validate them all.
		final var size = table.size();
		final var selected = projection == null ? null : projection.indices();
		final var count = selected == null ? size : selected.length;
		for (int i = 0; i < count; i++) {
			final var index = selected == null ? i : selected[i];
			final var column = table.get(index);
			final var isStatic = column instanceof ColumnMetadata metadata && metadata.isStatic();
			if (!partitionKey.contains(column.getName()) && !isStatic) {
				throw new InvalidQueryException(coordinator,
					("SELECT DISTINCT queries must only request partition key columns and/or static "
						+ "columns (not %s)").formatted(column.getName().asInternal()));
			}
		}
	}

	private static int[] partitionKeyIndices(final Target target) {
		final var table = target.table();

		return target.partitionKeyNames().stream().mapToInt(table::firstIndexOf).toArray();
	}

	private static List<Object> partitionKeyValues(final SeaStarRow row, final int[] indices) {
		final List<Object> values = new ArrayList<>(indices.length);
		for (final var index : indices) {
			values.add(row.getObject(index));
		}

		return values;
	}

	private static Set<CqlIdentifier> indexedColumns(final SeaStarTable table) {
		final Set<CqlIdentifier> columns = new HashSet<>();
		for (final var index : table.getIndexes().values()) {
			columns.add(CqlIdentifier.fromInternal(indexTargetColumn(index.getTarget())));
		}

		return columns;
	}

	private static String indexTargetColumn(final String target) {
		// Collection index targets look like values(col)/keys(col)/entries(col)/full(col); a simple
		// index target is just the column name.
		final var open = target.indexOf('(');
		if (open >= 0 && target.endsWith(")")) {
			return target.substring(open + 1, target.length() - 1);
		}

		return target;
	}

	/**
	 * The test a row has to pass to be returned, or null when the statement restricts nothing.
	 *
	 * <p>A restriction on a column that is neither part of the primary key nor served by a secondary
	 * index means a scan, which Cassandra refuses without ALLOW FILTERING.
	 */
	private static Predicate<SeaStarRow> resolveWhere(final Query query, final Node coordinator) {
		if (query.restrictions().isEmpty()) {
			return null;
		}

		final var target = query.target();
		final var primaryKey = target.primaryKeyNames();
		final var indexed = indexedColumns(target.table());
		final List<Predicate<SeaStarRow>> predicates = new ArrayList<>();
		for (final var restriction : query.restrictions()) {
			// A secondary index permits an equality restriction on its column without ALLOW
			// FILTERING; other non-primary-key restrictions still require it.
			final var indexedEquality = indexed.contains(restriction.column())
				&& restriction.operator() == CqlOperator.EQ;
			if (!primaryKey.contains(restriction.column()) && !indexedEquality
				&& !query.allowFiltering()) {
				throw new InvalidQueryException(coordinator,
					"Cannot execute this query as it might involve data filtering and thus may have "
						+ "unpredictable performance. If you want to execute this query despite the "
						+ "performance unpredictability, use ALLOW FILTERING");
			}
			predicates.add(restriction.toPredicate());
		}

		return predicates.stream().reduce(Predicate::and)
			.orElseThrow(() -> new IllegalStateException(
				"a WHERE clause with relations must yield at least one predicate"));
	}

	private static Row project(final Row source, final Projection projection) {
		final var definitions = projection.definitions();
		final var indices = projection.indices();

		return new Row() {

			@Override
			public boolean isDetached() {
				return source.isDetached();
			}

			@Override
			public void attach(final @NonNull AttachmentPoint attachmentPoint) {
				throw new UnsupportedOperationException();
			}

			@Override
			@NonNull
			public CodecRegistry codecRegistry() {
				return source.codecRegistry();
			}

			@Override
			@NonNull
			public ProtocolVersion protocolVersion() {
				return source.protocolVersion();
			}

			@Override
			public int size() {
				return definitions.size();
			}

			@Override
			@NonNull
			public DataType getType(final int i) {
				return definitions.get(i).getType();
			}

			@Override
			public ByteBuffer getBytesUnsafe(final int i) {
				return source.getBytesUnsafe(indices[i]);
			}

			@Override
			public int firstIndexOf(final @NonNull String name) {
				return definitions.firstIndexOf(name);
			}

			@Override
			@NonNull
			public DataType getType(final @NonNull String name) {
				return definitions.get(name).getType();
			}

			@Override
			public int firstIndexOf(final @NonNull CqlIdentifier id) {
				return definitions.firstIndexOf(id);
			}

			@Override
			@NonNull
			public DataType getType(final @NonNull CqlIdentifier id) {
				return definitions.get(id).getType();
			}

			@Override
			@NonNull
			public ColumnDefinitions getColumnDefinitions() {
				return definitions;
			}

		};
	}

}
