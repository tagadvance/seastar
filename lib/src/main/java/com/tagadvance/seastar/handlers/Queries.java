package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarDriverContext;
import com.tagadvance.seastar.SeaStarTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.SelectStatement.RawStatement;
import org.jspecify.annotations.Nullable;

/**
 * Translates a parsed SELECT into a {@link Query}. Together with {@link Modifications} this is the
 * boundary: below it the {@code org.apache.cassandra} parse tree, above it handlers that only see
 * columns, values and operators.
 */
final class Queries {

	private Queries() {
		// hidden constructor
	}

	static Query translate(final SeaStarDriverContext context,
		final Supplier<Optional<CqlIdentifier>> sessionKeyspace, final RawStatement raw,
		final Node coordinator, final Object... bindings) {
		final var target = Targets.require(context, sessionKeyspace, raw, coordinator);
		final var table = target.table();
		final var codecRegistry = context.getCodecRegistry();

		final var projection = projection(table, raw.selectClause, coordinator);
		final var restrictions = Restrictions.translate(table, raw.whereClause, codecRegistry,
			coordinator, bindings);
		final var orderBy = orderBy(table, raw, coordinator);
		final var limit = limit(raw.limit, codecRegistry, coordinator, bindings);

		return new Query(target, projection, raw.parameters.isDistinct, raw.parameters.allowFiltering,
			restrictions, orderBy, limit);
	}

	/**
	 * The ORDER BY clause as written, with each column resolved against the table. A column that
	 * does not exist is reported here for the same reason a selected one is: it is wrong whatever
	 * the table holds.
	 */
	private static List<Sort> orderBy(final SeaStarTable table, final RawStatement raw,
		final Node coordinator) {
		final List<Sort> orderBy = new ArrayList<>(raw.parameters.orderings.size());
		for (final var ordering : raw.parameters.orderings) {
			final var expression = FieldBindings.ORDERING_EXPRESSION.require(ordering);
			// The other expression is ANN, which orders by distance from a vector rather than by a
			// stored value and only ever runs off a vector index.
			if (!(expression instanceof Ordering.Raw.SingleColumn)) {
				throw new InvalidQueryException(coordinator,
					"Unsupported ORDER BY expression %s".formatted(expression));
			}
			final var name = CqlIdentifier.fromInternal(
				FieldBindings.ORDERING_COLUMN.require(expression).toString());
			if (table.firstIndexOf(name) < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(name.asInternal()));
			}
			final var direction = FieldBindings.ORDERING_DIRECTION.require(ordering);
			orderBy.add(new Sort(name, direction == Ordering.Direction.DESC));
		}

		return List.copyOf(orderBy);
	}

	/**
	 * The positions of the selected columns, or an empty list for {@code SELECT *}, which selects
	 * the table as it stands rather than a fixed set of columns.
	 */
	private static List<Integer> projection(final SeaStarTable table,
		final List<RawSelector> selectClause, final Node coordinator) {
		final List<Integer> indices = new ArrayList<>(selectClause.size());
		for (final var selector : selectClause) {
			if (!(selector.selectable instanceof Selectable.RawIdentifier identifier)) {
				throw new UnsupportedOperationException(
					"Unsupported select item %s".formatted(selector.selectable));
			}
			final var name = Selectables.toIdentifier(identifier);
			final var index = table.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Undefined column name %s".formatted(name.asInternal()));
			}
			indices.add(index);
		}

		return List.copyOf(indices);
	}

	/**
	 * A LIMIT has to be a positive whole number whether or not the table it reads has any rows, so
	 * it is validated here rather than by the handler.
	 */
	private static @Nullable Integer limit(final Term.@Nullable Raw limit,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (limit == null) {
			return null;
		}

		final var value = Terms.resolve(limit, DataTypes.INT, codecRegistry, coordinator, bindings);
		if (!(value instanceof Number number)) {
			throw new InvalidQueryException(coordinator, "Invalid limit value %s".formatted(value));
		}
		final var rows = number.intValue();
		if (rows <= 0) {
			throw new InvalidQueryException(coordinator, "LIMIT must be strictly positive");
		}

		return rows;
	}

}
