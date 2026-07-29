package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * One column a statement writes, resolved against the table it writes to: an INSERT value, an
 * UPDATE {@code SET} item, or a column or part of one that a DELETE clears.
 *
 * <p>Every form is a function from the column's current value to its new one, so a handler applies
 * a write the same way whatever the statement said - which is also why {@code DELETE name} and
 * {@code SET name = null} share one path.
 *
 * <p>Which forms a column will accept is {@link Modifications}' business, because it is a question
 * about the column's type: only a list can be prepended to, only a counter incremented, only an
 * unfrozen user defined type given a field.
 *
 * @param columnIndex the position of the assigned column in the table
 * @param column      the name of the assigned column
 * @param operator    what the statement does to the column
 * @param selector    the element, key, index or field the operator addresses, where it addresses
 *                    one; null otherwise
 * @param value       the value the operator writes, where it writes one; null otherwise
 */
record Assignment(int columnIndex, CqlIdentifier column, Assignment.Operator operator,
				  @Nullable Object selector, @Nullable Object value) {

	/**
	 * What an assignment does to the column it names.
	 */
	enum Operator {

		/** {@code c = v}, and the {@code v = null} a {@code DELETE c} becomes. */
		SET,
		/** {@code c = c + v} on a list, set or map. */
		APPEND,
		/** {@code c = v + c}, which only a list accepts. */
		PREPEND,
		/** {@code c = c - v}: elements of a list or set, keys of a map. */
		DISCARD,
		/** {@code c = c + n} on a counter. */
		INCREMENT,
		/** {@code c = c - n} on a counter. */
		DECREMENT,
		/** {@code c[i] = v} on a list. */
		SET_LIST_ELEMENT,
		/** {@code c[k] = v} on a map. */
		SET_MAP_ENTRY,
		/** {@code c.f = v} on an unfrozen user defined type. */
		SET_FIELD,
		/** {@code DELETE c[i]} on a list. */
		DELETE_LIST_ELEMENT,
		/** {@code DELETE c[e]} on a set. */
		DELETE_SET_ELEMENT,
		/** {@code DELETE c[k]} on a map. */
		DELETE_MAP_ENTRY,
		/** {@code DELETE c.f} on an unfrozen user defined type. */
		DELETE_FIELD

	}

	/**
	 * The plain {@code column = value} form, which is what an INSERT writes and what a DELETE of a
	 * named column becomes.
	 */
	static Assignment set(final int columnIndex, final CqlIdentifier column,
		final @Nullable Object value) {
		return new Assignment(columnIndex, column, Operator.SET, null, value);
	}

	/**
	 * The value this assignment leaves in the column.
	 *
	 * @param current the value the column holds now, or null for a row that does not exist yet
	 */
	@Nullable
	Object apply(final @Nullable Object current, final Node coordinator) {
		return switch (operator) {
			case SET -> value;
			case APPEND -> append(current);
			case PREPEND -> prepend(current);
			case DISCARD -> discard(current);
			case INCREMENT -> counter(current) + delta();
			case DECREMENT -> counter(current) - delta();
			case SET_LIST_ELEMENT -> setListElement(current, coordinator);
			case SET_MAP_ENTRY -> entries(current, map -> map.put(selector, value));
			case SET_FIELD -> setField(current, value);
			case DELETE_LIST_ELEMENT -> deleteListElement(current, coordinator);
			case DELETE_SET_ELEMENT -> elements(current, set -> set.remove(selector));
			case DELETE_MAP_ENTRY -> entries(current, map -> map.remove(selector));
			case DELETE_FIELD -> setField(current, null);
		};
	}

	/**
	 * An empty collection literal resolves to null, so {@code c = c + []} adds nothing rather than
	 * clearing the column.
	 */
	private @Nullable Object append(final @Nullable Object current) {
		if (value == null) {
			return current;
		}
		if (value instanceof Map<?, ?> added) {
			final Map<Object, Object> result = new LinkedHashMap<>();
			if (current instanceof Map<?, ?> existing) {
				result.putAll(existing);
			}
			result.putAll(added);

			return result;
		}
		if (value instanceof Set<?> added) {
			final Set<Object> result = new LinkedHashSet<>();
			if (current instanceof Collection<?> existing) {
				result.addAll(existing);
			}
			result.addAll(added);

			return result;
		}

		final List<Object> result = new ArrayList<>();
		if (current instanceof Collection<?> existing) {
			result.addAll(existing);
		}
		result.addAll((Collection<?>) value);

		return result;
	}

	private @Nullable Object prepend(final @Nullable Object current) {
		if (value == null) {
			return current;
		}

		final List<Object> result = new ArrayList<>((Collection<?>) value);
		if (current instanceof Collection<?> existing) {
			result.addAll(existing);
		}

		return result;
	}

	/**
	 * Removing from a list drops every occurrence of each named element, not the first; removing
	 * from a map is written as a set of keys.
	 */
	private @Nullable Object discard(final @Nullable Object current) {
		if (value == null || current == null) {
			return current;
		}
		if (current instanceof Map<?, ?> existing) {
			final Map<Object, Object> result = new LinkedHashMap<>(existing);
			((Collection<?>) value).forEach(result::remove);

			return result;
		}
		if (current instanceof Set<?> existing) {
			final Set<Object> result = new LinkedHashSet<>(existing);
			result.removeAll((Collection<?>) value);

			return result;
		}

		final List<Object> result = new ArrayList<>((Collection<?>) current);
		result.removeAll((Collection<?>) value);

		return result;
	}

	/**
	 * A counter that has never been written reads as null and counts as zero, which is what makes
	 * the first {@code n = n + 1} on a row work.
	 */
	private static long counter(final @Nullable Object current) {
		return current instanceof Number number ? number.longValue() : 0L;
	}

	private long delta() {
		return value instanceof Number number ? number.longValue() : 0L;
	}

	private Object setListElement(final @Nullable Object current, final Node coordinator) {
		final var result = list(current);
		result.set(requireIndex(result.size(), coordinator), value);

		return result;
	}

	private Object deleteListElement(final @Nullable Object current, final Node coordinator) {
		final var result = list(current);
		result.remove(requireIndex(result.size(), coordinator));

		return result;
	}

	private int requireIndex(final int size, final Node coordinator) {
		final var index = ((Number) selector).intValue();
		if (index < 0 || index >= size) {
			throw new InvalidQueryException(coordinator,
				"List index %d out of bound, list %s has size %d".formatted(index,
					column.asInternal(), size));
		}

		return index;
	}

	private List<Object> list(final @Nullable Object current) {
		return current instanceof Collection<?> existing ? new ArrayList<>(existing)
			: new ArrayList<>();
	}

	private static Object entries(final @Nullable Object current,
		final Consumer<Map<Object, Object>> edit) {
		final Map<Object, Object> result = current instanceof Map<?, ?> existing
			? new LinkedHashMap<>(existing) : new LinkedHashMap<>();
		edit.accept(result);

		return result;
	}

	private static Object elements(final @Nullable Object current,
		final Consumer<Set<Object>> edit) {
		final Set<Object> result = current instanceof Collection<?> existing
			? new LinkedHashSet<>(existing) : new LinkedHashSet<>();
		edit.accept(result);

		return result;
	}

	/**
	 * Writing a field leaves the stored value alone and returns a new one, because the value a row
	 * holds may be shared with a snapshot already handed to a caller.
	 */
	private @Nullable Object setField(final @Nullable Object current,
		final @Nullable Object fieldValue) {
		if (!(current instanceof UdtValue existing)) {
			return current;
		}

		final var copy = existing.getType().newValue();
		for (int i = 0; i < existing.size(); i++) {
			copy.setBytesUnsafe(i, existing.getBytesUnsafe(i));
		}
		final var index = ((Number) selector).intValue();
		final TypeCodec<Object> codec = copy.codecRegistry().codecFor(copy.getType(index));
		copy.set(index, fieldValue, codec);

		return copy;
	}

}
