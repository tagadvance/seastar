package com.tagadvance.seastar.handlers;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.tagadvance.seastar.SeaStarAsyncResultSet;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
		final Predicate<SeaStarRow> predicate;
		final int[] distinctKey;
		final RowOrdering ordering;
		try {
			query = Queries.translate(context, getKeyspace, raw, coordinator, bindings);
			if (query.distinct()) {
				validateDistinct(query, coordinator);
			}
			predicate = RestrictionRules.forSelect(query, coordinator);
			distinctKey = query.distinct() ? partitionKeyIndices(query.target()) : null;
			ordering = RowOrdering.of(query.target().table(), reversed(query, coordinator));
		} catch (final InvalidQueryException e) {
			return CompletableFuture.failedStage(e);
		}

		final var table = query.target().table();
		final var limit = query.limit();

		return table.query(() -> {
			// A row whose marker and every cell have expired is gone, as it is on a cluster; expiry is
			// evaluated here rather than reaped on a timer, so nothing depends on wall-clock progress.
			var rows = table.rows().filter(SeaStarRow::isLive);
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
			if (limit != null && !query.isAggregate()) {
				rows = rows.limit(limit);
			}

			final var result = query.isAggregate() ? aggregate(context, query, rows)
				: project(context, query, rows);

			return CompletableFuture.<AsyncResultSet>completedStage(
				new SeaStarAsyncResultSet(result.definitions(), executionInfo, result.rows()));
		});
	}

	/**
	 * The result of a query: the columns it reports and the rows it answers with.
	 */
	private record Result(ColumnDefinitions definitions, LinkedList<Row> rows) {

	}

	/**
	 * The definitions a query reports, which for {@code SELECT *} are the table's own and otherwise
	 * the selectors themselves - a selector <em>is</em> a column definition, which is what carries an
	 * alias through to the result.
	 */
	private static ColumnDefinitions definitions(final Query query) {
		final var table = query.target().table();
		if (query.json()) {
			return DefaultColumnDefinitions.valueOf(
				List.of(new Selector(table.getKeyspace(), table.getName(), Jsons.COLUMN,
					DataTypes.TEXT, null, null, row -> null)));
		}
		if (query.isWildcard()) {
			return table.snapshot();
		}

		return DefaultColumnDefinitions.valueOf(
			query.selectors().stream().map(ColumnDefinition.class::cast).toList());
	}

	/**
	 * A row-wise query: every matched row produces one result row.
	 */
	private static Result project(final SeaStarDriverContext context, final Query query,
		final Stream<SeaStarRow> rows) {
		final var definitions = definitions(query);
		if (query.isWildcard() && !query.json()) {
			return new Result(definitions,
				rows.map(SeaStarRow::snapshot).collect(Collectors.toCollection(LinkedList::new)));
		}

		final var names = names(query);
		final var types = types(query);
		final var readers = readers(query);
		final var data = rows.map(row -> {
			final List<Object> values = new ArrayList<>(readers.size());
			readers.forEach(reader -> values.add(reader.read(row)));

			return row(context, query, definitions, names, types, values);
		}).collect(Collectors.toCollection(LinkedList::new));

		return new Result(definitions, data);
	}

	/**
	 * An aggregating query: however many rows matched, the answer is exactly one row - including when
	 * none did, which is why {@code SELECT count(*)} on an empty table answers zero rather than
	 * nothing.
	 */
	private static Result aggregate(final SeaStarDriverContext context, final Query query,
		final Stream<SeaStarRow> rows) {
		final var definitions = definitions(query);
		final var codecRegistry = context.getCodecRegistry();
		final var version = context.getProtocolVersion();
		final var selectors = query.selectors();
		final List<Aggregation> aggregations = selectors.stream()
			.map(selector -> selector.isAggregate()
				? new Aggregation(selector, codecRegistry, version) : null)
			.toList();
		// A plain column alongside an aggregate reports the first matched row's value, which is what a
		// cluster answers for SELECT count(*), v.
		final List<Object> first = new ArrayList<>(java.util.Collections.nCopies(selectors.size(),
			null));
		final var seen = new boolean[]{false};

		rows.forEach(row -> {
			for (int i = 0; i < selectors.size(); i++) {
				final var aggregation = aggregations.get(i);
				if (aggregation != null) {
					aggregation.accumulate(row);
				} else if (!seen[0]) {
					first.set(i, selectors.get(i).reader().read(row));
				}
			}
			seen[0] = true;
		});

		final List<Object> values = new ArrayList<>(selectors.size());
		for (int i = 0; i < selectors.size(); i++) {
			final var aggregation = aggregations.get(i);
			values.add(aggregation == null ? first.get(i) : aggregation.result());
		}

		final var data = new LinkedList<Row>();
		data.add(row(context, query, definitions, names(query), types(query), values));

		return new Result(definitions, data);
	}

	/**
	 * One result row, wrapped into the single {@code [json]} column when {@code SELECT JSON} asked
	 * for it.
	 */
	private static Row row(final SeaStarDriverContext context, final Query query,
		final ColumnDefinitions definitions, final List<CqlIdentifier> names,
		final List<DataType> types, final List<Object> values) {
		final var row = query.json()
			? List.<Object>of(Jsons.encode(names, types, values)) : values;

		return new ValueRow(definitions, row, context.getCodecRegistry(),
			context.getProtocolVersion());
	}

	private static List<Selector.Reader> readers(final Query query) {
		if (query.isWildcard()) {
			final var table = query.target().table();

			return wildcardIndices(table).stream()
				.<Selector.Reader>map(index -> row -> row.getObject(index))
				.toList();
		}

		return query.selectors().stream().map(Selector::reader).toList();
	}

	private static List<CqlIdentifier> names(final Query query) {
		if (query.isWildcard()) {
			final var table = query.target().table();

			return wildcardIndices(table).stream().map(index -> table.get(index).getName()).toList();
		}

		return query.selectors().stream().map(Selector::name).toList();
	}

	private static List<DataType> types(final Query query) {
		if (query.isWildcard()) {
			final var table = query.target().table();

			return wildcardIndices(table).stream().map(index -> table.get(index).getType()).toList();
		}

		return query.selectors().stream().map(Selector::type).toList();
	}

	private static List<Integer> wildcardIndices(final SeaStarTable table) {
		return java.util.stream.IntStream.range(0, table.size()).boxed().toList();
	}

	private static void validateDistinct(final Query query, final Node coordinator) {
		final var table = query.target().table();
		final var partitionKey = query.target().partitionKeyNames();
		final List<Integer> selected = query.isWildcard() ? wildcardIndices(table)
			: query.selectors().stream().map(Selector::columnIndex).toList();
		for (int i = 0; i < selected.size(); i++) {
			final var index = selected.get(i);
			final var name = index == null ? query.selectors().get(i).name()
				: table.get(index).getName();
			final var column = index == null ? null : table.get(index);
			final var isStatic = column instanceof ColumnMetadata metadata && metadata.isStatic();
			if (index == null || !partitionKey.contains(name) && !isStatic) {
				throw new InvalidQueryException(coordinator,
					("SELECT DISTINCT queries must only request partition key columns and/or static "
						+ "columns (not %s)").formatted(name.asInternal()));
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

	/**
	 * Whether an ORDER BY asked for the clustering order backwards, and the place every rule about
	 * writing one is enforced.
	 *
	 * <p>Cassandra stores a partition in clustering order and reads it in one direction or the
	 * other, so ORDER BY selects between those two readings rather than sorting anything: it is
	 * allowed only on a query that reads a single partition, only on clustering columns, only in
	 * their declared order, and only if every element agrees on the direction relative to the order
	 * the table declares.
	 */
	private static boolean reversed(final Query query, final Node coordinator) {
		final var orderBy = query.orderBy();
		if (orderBy.isEmpty()) {
			return false;
		}

		requireSinglePartition(query, coordinator);

		final var clustering = query.target().table().getClusteringColumns();
		final List<CqlIdentifier> names = clustering.keySet()
			.stream()
			.map(ColumnMetadata::getName)
			.toList();
		final var declared = List.copyOf(clustering.values());

		Boolean reversed = null;
		var expected = 0;
		// A column written twice is one ordering, as it is for Cassandra, which keys them by column.
		for (final var sort : new LinkedHashSet<>(orderBy)) {
			final var position = names.indexOf(sort.column());
			if (position < 0) {
				throw new InvalidQueryException(coordinator,
					("Order by is currently only supported on the clustered columns of the PRIMARY "
						+ "KEY, got %s").formatted(sort.column().asInternal()));
			}
			// A clustering column may be skipped only when it is pinned to one value anyway.
			while (expected < position && isRestrictedByEq(query, names.get(expected))) {
				expected++;
			}
			if (expected != position) {
				throw new InvalidQueryException(coordinator,
					"Order by currently only supports the ordering of columns following their "
						+ "declared order in the PRIMARY KEY");
			}
			expected++;

			final var flipped = sort.descending() != (declared.get(position) == ClusteringOrder.DESC);
			if (reversed == null) {
				reversed = flipped;
			} else if (reversed != flipped) {
				throw new InvalidQueryException(coordinator, "Unsupported order by relation");
			}
		}

		return reversed != null && reversed;
	}

	/**
	 * ORDER BY reads one partition in one direction, so every partition key column has to be pinned.
	 *
	 * <p>An IN on the partition key is pinned but not single, and Cassandra can only serve it by
	 * merging partitions in memory - which it refuses to do for a paged query, and every query the
	 * driver sends is paged by default. Refusing it here rather than answering it keeps a test that
	 * passes against SeaStar from failing against a cluster.
	 */
	private static void requireSinglePartition(final Query query, final Node coordinator) {
		var in = false;
		for (final var column : query.target().partitionKeyNames()) {
			final var operator = query.restrictions()
				.stream()
				.filter(restriction -> restriction.column().name().equals(column))
				.map(Restriction::operator)
				.findFirst();
			if (operator.filter(o -> o == CqlOperator.EQ || o == CqlOperator.IN).isEmpty()) {
				throw new InvalidQueryException(coordinator, "ORDER BY is only supported when the "
					+ "partition key is restricted by an EQ or an IN.");
			}
			in |= operator.get() == CqlOperator.IN;
		}
		if (in) {
			throw new InvalidQueryException(coordinator,
				"Cannot page queries with both ORDER BY and a IN restriction on the partition key; "
					+ "you must either remove the ORDER BY or the IN and sort client side, or "
					+ "disable paging for this query");
		}
	}

	private static boolean isRestrictedByEq(final Query query, final CqlIdentifier column) {
		return query.restrictions()
			.stream()
			.anyMatch(restriction -> restriction.column().name().equals(column)
				&& restriction.operator() == CqlOperator.EQ);
	}

}
