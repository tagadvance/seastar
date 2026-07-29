package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.tagadvance.seastar.SeaStarRow;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * One item of a SELECT clause, resolved against the table it reads: the column it produces and how
 * to compute that column's value from a row.
 *
 * <p>A selector is also the {@link ColumnDefinition} the result set reports, which is what carries
 * an alias through: {@code SELECT ck AS c} differs from {@code SELECT ck} only in the name here.
 *
 * @param name        the name the result column carries - the alias where one was written, and
 *                    otherwise the name a cluster derives ({@code system.min(v)}, {@code ttl(v)})
 * @param type        the type of the result column, which for an aggregate is the type it returns
 *                    rather than the type it reads
 * @param columnIndex the position of the column read, when the selector is a plain column and
 *                    nothing else; null otherwise. DISTINCT and {@code SELECT JSON} both need to
 *                    tell a column from an expression over one
 * @param aggregate   the aggregate to fold the read values with, or null for a row-wise selector
 * @param reader      the value read from one row: the result for a row-wise selector, the input for
 *                    an aggregate
 */
record Selector(CqlIdentifier keyspace, CqlIdentifier table, CqlIdentifier name, DataType type,
				@Nullable Integer columnIndex, Selector.@Nullable Aggregate aggregate,
				Selector.Reader reader) implements ColumnDefinition {

	/**
	 * The aggregates CQL defines over a result set. Each folds one value per row into one value for
	 * the whole query, so a SELECT that uses any of them returns exactly one row.
	 */
	enum Aggregate {

		/** {@code count(*)}, which counts rows rather than values. */
		COUNT_ROWS,
		/** {@code count(c)}, which counts the rows where {@code c} is not null. */
		COUNT,
		/** {@code min(c)}, compared in the order the column's type defines. */
		MIN,
		/** {@code max(c)}. */
		MAX,
		/** {@code sum(c)}, in the column's own numeric type. */
		SUM,
		/** {@code avg(c)}, in the column's own numeric type - so an int column averages to an int. */
		AVG

	}

	@FunctionalInterface
	interface Reader {

		@Nullable
		Object read(SeaStarRow row);

	}

	boolean isAggregate() {
		return aggregate != null;
	}

	@Override
	@NonNull
	public CqlIdentifier getKeyspace() {
		return keyspace;
	}

	@Override
	@NonNull
	public CqlIdentifier getTable() {
		return table;
	}

	@Override
	@NonNull
	public CqlIdentifier getName() {
		return name;
	}

	@Override
	@NonNull
	public DataType getType() {
		return type;
	}

	@Override
	public boolean isDetached() {
		return false;
	}

	@Override
	public void attach(final @NonNull AttachmentPoint attachmentPoint) {
		// A selector holds no encoded value of its own, so there is nothing to re-attach.
	}

}
