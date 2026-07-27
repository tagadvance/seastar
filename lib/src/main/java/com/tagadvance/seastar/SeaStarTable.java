package com.tagadvance.seastar;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.tagadvance.tools.SeaStarReadWriteLock;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.jspecify.annotations.NonNull;

/**
 * A table, its column list and its rows.
 *
 * <p>A table carries no lock of its own; {@link #lock()} is its keyspace's, and it guards the column
 * list, the key definition, the indexes and every row. See the lock hierarchy in {@code AGENTS.md}.
 */
public interface SeaStarTable extends SeaStarReadWriteLock, TableMetadata, ColumnDefinitions {

	SeaStarDriverContext context();

	SeaStarKeyspace keyspace();

	/**
	 * Runs a statement's mutation under the whole hierarchy: a read lock on the context, so the
	 * keyspace cannot be dropped underneath it, and the keyspace's write lock, which is the only lock
	 * anything inside the keyspace has. This is the order every mutation takes, and taking it in one
	 * place is what keeps handlers from inventing another.
	 */
	default <V> V mutate(final Callable<V> callable) {
		return context().readLockUnchecked(() -> writeLockUnchecked(callable));
	}

	/**
	 * Runs a statement's read under the whole hierarchy: a read lock on the context and one on the
	 * keyspace.
	 *
	 * @see #mutate(Callable)
	 */
	default <V> V query(final Callable<V> callable) {
		return context().readLockUnchecked(() -> readLockUnchecked(callable));
	}

	default SeaStarColumn addColumn(final @NonNull String name, final @NonNull DataType type) {
		return addColumn(CqlIdentifier.fromInternal(name), type);
	}

	default SeaStarColumn addColumn(final @NonNull CqlIdentifier name,
		final @NonNull DataType type) {
		return addColumn(name, type, false);
	}

	default SeaStarColumn addColumn(final @NonNull CqlIdentifier name, final @NonNull DataType type,
		final boolean isStatic) {
		final var column = new VolatileColumn(context(), this, name, type, isStatic);
		addColumn(column);

		return column;
	}

	void addColumn(final SeaStarColumn column);

	/**
	 * Adds a column where a live node keeps it - after the primary key columns, in alphabetical order
	 * among the rest - and opens a slot for it in every existing row, filled with {@code null}. This
	 * is {@code ALTER TABLE ... ADD}; {@link #addColumn(SeaStarColumn)} appends and is for building a
	 * table that has no rows yet.
	 */
	SeaStarColumn insertColumn(CqlIdentifier name, DataType type, boolean isStatic);

	/**
	 * Removes a column and its slot from every existing row, discarding the values it held. This is
	 * {@code ALTER TABLE ... DROP}; re-adding the column afterwards brings it back empty, as it does
	 * on a live node.
	 */
	void removeColumn(CqlIdentifier name);

	/**
	 * Renames a column in place, keeping its position, its type and its place in the partition or
	 * clustering key. Row values are positional, so nothing about them changes.
	 */
	void renameColumn(CqlIdentifier from, CqlIdentifier to);

	/**
	 * Marks an already-added column as part of the partition key. Partition key columns are ordered
	 * in the sequence they are marked.
	 */
	void markPartitionKey(CqlIdentifier name);

	/**
	 * Marks an already-added column as a clustering column with the given order. Clustering columns
	 * are ordered in the sequence they are marked.
	 */
	void markClustering(CqlIdentifier name, ClusteringOrder order);

	default SeaStarRow addRow(final Object... values) {
		return addRow(List.of(values));
	}

	/**
	 * Validating the values against the column list and adding the row are one locked region: a
	 * schema change in between would leave a row validated against a column list that no longer
	 * exists.
	 */
	default SeaStarRow addRow(final List<Object> values) {
		return writeLockUnchecked(() -> {
			final var row = new VolatileRow(context(), this, values);
			addRow(row);

			return row;
		});
	}

	/**
	 * Adds a row whose cells carry the write time a statement asked for rather than the clock's, so
	 * that {@code INSERT ... USING TIMESTAMP} is what {@code writetime()} reports back.
	 */
	default SeaStarRow addRow(final List<Object> values, final long writeTime) {
		return writeLockUnchecked(() -> {
			final var row = new VolatileRow(context(), this, values, writeTime);
			addRow(row);

			return row;
		});
	}

	/**
	 * Whether the table declares any static column, and so whether a partition holds cells of its own
	 * beside its rows.
	 */
	default boolean hasStaticColumns() {
		return getColumns().values().stream().anyMatch(ColumnMetadata::isStatic);
	}

	void addRow(final SeaStarRow row);

	void removeRowIf(Predicate<SeaStarRow> predicate);

	/**
	 * Removes the matching rows of one partition, leaving the rest of the table untouched. A DELETE
	 * that pins the partition key knows which partition it is writing to, so it need not walk the
	 * table to find out.
	 *
	 * @param partitionKeyValues one value per partition key column, in key order
	 */
	void removeRowIf(List<Object> partitionKeyValues, Predicate<SeaStarRow> predicate);

	/**
	 * Records a secondary index on this table, keyed by its name. Exposed through
	 * {@link #getIndexes()}.
	 */
	void addIndex(IndexMetadata index);

	/**
	 * Forgets the index of that name, if this table holds one.
	 */
	void removeIndex(CqlIdentifier name);

	/**
	 * Every row of the table, as a snapshot rather than a live view.
	 */
	Stream<SeaStarRow> rows();

	/**
	 * The rows of one partition, as a snapshot. A statement whose WHERE clause pins every partition
	 * key column can answer itself from here rather than from {@link #rows()}, which is the
	 * difference between a walk of one partition and a scan of the table.
	 *
	 * @param partitionKeyValues one value per partition key column, in key order
	 */
	Stream<SeaStarRow> partition(List<Object> partitionKeyValues);

	void drop();

	void truncate();

	ColumnDefinitions snapshot();

}
