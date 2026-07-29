package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.tagadvance.seastar.SeaStarRow;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * One relation of a WHERE clause, resolved against the table it restricts: which columns it names,
 * the comparison it applies, and the values it compares against, already converted from terms to
 * the Java values SeaStar stores.
 *
 * <p>A relation restricts one column ({@code ck > 1}) or several at once
 * ({@code (ck1, ck2) > (1, 2)}), so the model is the same shape for both: a list of columns and a
 * list of value tuples of that arity. A scalar comparison carries one tuple, an IN carries one per
 * alternative, and an operator that takes no term carries none.
 *
 * <p>Whether a restriction is <em>allowed</em> is {@link RestrictionRules}' business, because it is
 * a rule about the table rather than about the relation. Whether a restriction <em>matches a
 * row</em> is the same question for every statement, so it is answered once, here.
 *
 * @param columns  the restricted columns, in the order the relation names them
 * @param operator the comparison to apply
 * @param values   one tuple per alternative, each holding one value per restricted column
 */
record Restriction(List<Column> columns, CqlOperator operator, List<List<Object>> values) {

	/**
	 * One restricted column. Evaluating a restriction needs where the value sits in a row and what
	 * type it is, because ordering is the column type's answer rather than {@link Object}'s.
	 *
	 * @param index the position of the column in the table
	 * @param name  the name of the column
	 * @param type  the type of the column
	 */
	record Column(int index, CqlIdentifier name, DataType type) {

	}

	Restriction {
		columns = List.copyOf(columns);
		// A bound marker with no value resolves to null, so the tuples have to tolerate one.
		values = values.stream()
			.map(tuple -> Collections.unmodifiableList(new ArrayList<>(tuple)))
			.toList();
	}

	/**
	 * A relation over several columns at once, which Cassandra allows only on clustering columns.
	 */
	boolean isMultiColumn() {
		return columns.size() > 1;
	}

	/**
	 * The single column a scalar relation restricts.
	 */
	Column column() {
		if (columns.size() != 1) {
			throw new IllegalStateException(
				"restriction with operator %s restricts %d columns, not one".formatted(operator,
					columns.size()));
		}

		return columns.get(0);
	}

	/**
	 * The single value a scalar comparison compares against.
	 */
	Object value() {
		final var tuple = values.size() == 1 ? values.get(0) : List.of();
		if (columns.size() != 1 || tuple.size() != 1) {
			throw new IllegalStateException(
				"restriction on %s with operator %s does not carry exactly one value".formatted(
					columns, operator));
		}

		return tuple.get(0);
	}

	/**
	 * The value this restriction pins each of its columns to, by column position, or nothing when it
	 * pins none. Only equality does; an UPDATE uses it to synthesize the row it upserts.
	 */
	Map<Integer, Object> equalityValues() {
		if (operator != CqlOperator.EQ) {
			return Map.of();
		}

		final Map<Integer, Object> pinned = new LinkedHashMap<>(columns.size());
		final var tuple = values.get(0);
		for (int i = 0; i < columns.size(); i++) {
			pinned.put(columns.get(i).index(), tuple.get(i));
		}

		return pinned;
	}

	/**
	 * The test a row has to pass to satisfy this restriction.
	 *
	 * @throws UnsupportedOperationException if SeaStar cannot evaluate this operator. The operators
	 *                                       that reach here are the ones no statement accepts, and
	 *                                       {@link RestrictionRules} rejects them by name first
	 */
	Predicate<SeaStarRow> toPredicate() {
		return switch (operator) {
			case EQ -> row -> matches(row, values.get(0));
			case IN -> row -> values.stream().anyMatch(tuple -> matches(row, tuple));
			case LT -> slice(comparison -> comparison < 0);
			case LTE -> slice(comparison -> comparison <= 0);
			case GT -> slice(comparison -> comparison > 0);
			case GTE -> slice(comparison -> comparison >= 0);
			case CONTAINS -> contains(false);
			case CONTAINS_KEY -> contains(true);
			case NEQ, IS_NOT, LIKE, ANN -> throw new UnsupportedOperationException(
				"Unsupported operator %s in WHERE".formatted(operator));
		};
	}

	private boolean matches(final SeaStarRow row, final List<Object> tuple) {
		for (int i = 0; i < columns.size(); i++) {
			if (!Objects.equals(row.getObject(columns.get(i).index()), tuple.get(i))) {
				return false;
			}
		}

		return true;
	}

	/**
	 * A multi-column bound compares lexicographically - {@code (a, b) > (1, 2)} is
	 * {@code a > 1 OR (a = 1 AND b > 2)} - and a single-column bound is the one-component case of
	 * the same walk. Clustering order does not enter into it: DESC reverses the order rows come back
	 * in, not the meaning of {@code >}.
	 */
	private Predicate<SeaStarRow> slice(final IntPredicate accept) {
		final var bound = values.get(0);
		final var comparators = columns.stream().map(column -> comparator(column.type())).toList();

		return row -> {
			for (int i = 0; i < columns.size(); i++) {
				final var actual = row.getObject(columns.get(i).index());
				final var expected = bound.get(i);
				// A row with nothing in the column cannot be inside any range.
				if (actual == null || expected == null) {
					return false;
				}
				final var comparison = comparators.get(i).compare(actual, expected);
				if (comparison != 0) {
					return accept.test(comparison);
				}
			}

			return accept.test(0);
		};
	}

	private Predicate<SeaStarRow> contains(final boolean key) {
		final var index = column().index();
		final var expected = value();

		return row -> {
			final var actual = row.getObject(index);
			if (actual instanceof Map<?, ?> map) {
				return key ? map.containsKey(expected) : map.containsValue(expected);
			}

			return actual instanceof Collection<?> collection && collection.contains(expected);
		};
	}

	/**
	 * Whether a value of this type can bound a range at all. A duration has no order, and neither
	 * does a collection or a user defined type, so Cassandra refuses to compare them.
	 */
	static boolean isOrderable(final DataType type, final CodecRegistry codecRegistry) {
		if (DataTypes.BLOB.equals(type) || DataTypes.INET.equals(type)) {
			return true;
		}

		return Comparable.class.isAssignableFrom(codecRegistry.codecFor(type).getJavaType()
			.getRawType());
	}

	/**
	 * Ordering is the column type's rather than the Java value's: a blob orders by unsigned bytes,
	 * where {@link ByteBuffer#compareTo} treats them as signed, and an inet address orders by its
	 * address, which {@link InetAddress} does not answer for at all.
	 */
	@SuppressWarnings("unchecked")
	private static Comparator<Object> comparator(final DataType type) {
		if (DataTypes.BLOB.equals(type)) {
			return (left, right) -> Arrays.compareUnsigned(bytes((ByteBuffer) left),
				bytes((ByteBuffer) right));
		}
		if (DataTypes.INET.equals(type)) {
			return (left, right) -> Arrays.compareUnsigned(((InetAddress) left).getAddress(),
				((InetAddress) right).getAddress());
		}

		return (left, right) -> ((Comparable<Object>) left).compareTo(right);
	}

	private static byte[] bytes(final ByteBuffer buffer) {
		final var copy = buffer.duplicate();
		final var bytes = new byte[copy.remaining()];
		copy.get(bytes);

		return bytes;
	}

}
