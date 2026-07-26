package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.tagadvance.seastar.SeaStarRow;
import com.tagadvance.seastar.SeaStarTable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import net.jcip.annotations.ThreadSafe;

/**
 * The order a table's rows come back in: partitions by Murmur3 token, rows within a partition by
 * the clustering columns in their declared order and declared direction.
 *
 * <p>Rows are ordered at read time rather than kept ordered on insert. There is no total order to
 * keep them in - a token is a property of the partition key, not of the row - and paying for it on
 * every write would make bulk loading a fixture, which is what SeaStar exists to make fast, worse
 * to make reads that are already a full scan slightly better.
 *
 * <p>The sort decorates each row with its token, its encoded key and its clustering values, so a
 * row is encoded once rather than once per comparison.
 */
@ThreadSafe
final class RowOrdering {

	private final int[] partitionKey;
	private final int[] clustering;
	private final Comparator<Decorated> comparator;

	private RowOrdering(final int[] partitionKey, final int[] clustering,
		final Comparator<Decorated> comparator) {
		this.partitionKey = partitionKey;
		this.clustering = clustering;
		this.comparator = comparator;
	}

	/**
	 * The ordering for {@code table}.
	 *
	 * @param reversed whether an ORDER BY asked for the clustering order backwards. It applies only
	 *                 within a partition, which costs nothing in fidelity: Cassandra permits ORDER
	 *                 BY only on a query that reads one partition.
	 */
	static RowOrdering of(final SeaStarTable table, final boolean reversed) {
		return table.readLockUnchecked(() -> {
			final var context = table.context();
			final var codecRegistry = context.getCodecRegistry();
			final var version = context.getProtocolVersion();

			final var partitionKey = table.getPartitionKey()
				.stream()
				.mapToInt(column -> table.firstIndexOf(column.getName()))
				.toArray();

			final var clusteringColumns = table.getClusteringColumns();
			final var clustering = clusteringColumns.keySet()
				.stream()
				.mapToInt(column -> table.firstIndexOf(column.getName()))
				.toArray();

			// Two distinct keys can share a token; Cassandra breaks that tie on the key itself.
			Comparator<Decorated> comparator = Comparator.comparingLong(Decorated::token)
				.thenComparing(Decorated::key, ValueComparators.unsignedBytes());
			var position = 0;
			for (final var entry : clusteringColumns.entrySet()) {
				final var index = position++;
				final var descending = entry.getValue() == ClusteringOrder.DESC ^ reversed;
				final var values = ValueComparators.of(entry.getKey().getType(), codecRegistry,
					version);
				comparator = comparator.thenComparing(decorated -> decorated.clustering().get(index),
					descending ? values.reversed() : values);
			}

			return new RowOrdering(partitionKey, clustering, comparator);
		});
	}

	/**
	 * Orders a stream of rows. A table with neither a partition key nor a clustering column - which
	 * only a test that builds one by hand can produce - has nothing to order by, and is left alone.
	 */
	Stream<SeaStarRow> sort(final Stream<SeaStarRow> rows) {
		if (partitionKey.length == 0 && clustering.length == 0) {
			return rows;
		}

		return rows.map(this::decorate).sorted(comparator).map(Decorated::row);
	}

	private Decorated decorate(final SeaStarRow row) {
		final List<ByteBuffer> components = new ArrayList<>(partitionKey.length);
		for (final var index : partitionKey) {
			components.add(row.getBytesUnsafe(index));
		}
		final var key = components.isEmpty() ? ByteBuffer.allocate(0) : Tokens.encode(components);

		final List<Object> values = new ArrayList<>(clustering.length);
		for (final var index : clustering) {
			values.add(row.getObject(index));
		}

		return new Decorated(Tokens.of(key), key, values, row);
	}

	/**
	 * A row with everything the comparison needs already computed.
	 */
	private record Decorated(long token, ByteBuffer key, List<Object> clustering, SeaStarRow row) {

	}

}
