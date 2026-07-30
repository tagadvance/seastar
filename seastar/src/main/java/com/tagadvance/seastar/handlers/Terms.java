package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.ArrayLiteral;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.TypeCast;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.functions.FunctionCall;
import org.jspecify.annotations.Nullable;

/**
 * Resolves a parsed term into the Java value it stands for, against the type of the column or field
 * it is assigned to or compared with. Shared by everything in the translation layer that reads
 * values out of a statement, so that a term means the same thing in an INSERT, an UPDATE, a WHERE
 * clause, and an IF condition.
 */
final class Terms {

	/**
	 * The functions SeaStar evaluates. Resolving a function properly means Cassandra's whole function
	 * machinery - overloads, argument coercion, user defined functions - so only the nullary ones
	 * that turn up in fixtures are answered, and everything else is rejected by name.
	 */
	private static final Map<String, Supplier<Object>> FUNCTIONS = Map.of("now", Uuids::timeBased,
		"uuid", UUID::randomUUID, "currenttimestamp", Instant::now);

	private Terms() {
	}

	/**
	 * A collection or UDT literal can nest further literals and bind markers, so terms resolve
	 * recursively against the type of the element, key, value or field they are being assigned to
	 * rather than only at the top level.
	 */
	static @Nullable Object resolve(final Term.Raw term, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (term instanceof AbstractMarker.Raw marker) {
			final var bindIndex = FieldBindings.MARKER_BIND_INDEX.require(marker);
			// A prepared statement may leave its trailing markers unbound, which reads as null.
			final var value = bindIndex < bindings.length ? bindings[bindIndex] : null;
			if (value != null && !codecRegistry.codecFor(dataType).accepts(value)) {
				throw new InvalidQueryException(coordinator,
					"Invalid value %s for a bind marker of type %s".formatted(value,
						dataType.asCql(true, true)));
			}

			return value;
		} else if (FieldBindings.NULL_LITERAL.isInstance(term)) {
			return null;
		} else if (term instanceof Constants.Literal literal) {
			return parse(literal.getText(), dataType, codecRegistry, coordinator);
		} else if (term instanceof UserTypes.Literal literal) {
			return toUdtValue(literal, dataType, codecRegistry, coordinator, bindings);
		} else if (term instanceof ArrayLiteral literal) {
			return toListOrVector(FieldBindings.ARRAY_ELEMENTS.require(literal), dataType, codecRegistry,
				coordinator, bindings);
		} else if (term instanceof Sets.Literal literal) {
			return toSetOrEmptyMap(FieldBindings.SET_ELEMENTS.require(literal), dataType, codecRegistry,
				coordinator, bindings);
		} else if (term instanceof Maps.Literal literal) {
			return toMap(literal, dataType, codecRegistry, coordinator, bindings);
		} else if (term instanceof Tuples.Literal literal) {
			return toTupleValue(FieldBindings.TUPLE_ELEMENTS.require(literal), dataType, codecRegistry,
				coordinator, bindings);
		} else if (term instanceof TypeCast cast) {
			return toCast(cast, dataType, codecRegistry, coordinator, bindings);
		} else if (term instanceof FunctionCall.Raw call) {
			return call(call, dataType, codecRegistry, coordinator);
		}

		throw new UnsupportedOperationException("Unsupported term %s".formatted(term));
	}

	/**
	 * Parses a literal against the type it is being assigned to. The driver's codecs report a
	 * malformed literal as {@link IllegalArgumentException}, which is a client-side signal; a live
	 * cluster rejects the same statement with an {@link InvalidQueryException}, so translate.
	 */
	private static @Nullable Object parse(final String text, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator) {
		try {
			return codecRegistry.codecFor(dataType).parse(text);
		} catch (final IllegalArgumentException e) {
			throw new InvalidQueryException(coordinator, e.getMessage());
		}
	}

	private static UdtValue toUdtValue(final UserTypes.Literal literal, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (!(dataType instanceof UserDefinedType udt)) {
			throw new InvalidQueryException(coordinator,
				"Invalid user type literal for a column of type %s".formatted(dataType.asCql(true, true)));
		}

		// Fields absent from the literal keep the null they were initialised with, matching
		// UserTypes.Literal.prepare, which substitutes NULL_LITERAL for anything not named.
		final var value = udt.newValue();
		literal.entries.forEach((field, fieldTerm) -> {
			final var name = CqlIdentifier.fromInternal(field.toString());
			final var index = udt.firstIndexOf(name);
			if (index < 0) {
				throw new InvalidQueryException(coordinator,
					"Unknown field '%s' in value of user defined type %s".formatted(name.asInternal(),
						udt.getName().asInternal()));
			}
			final var fieldType = udt.getFieldTypes().get(index);
			final TypeCodec<Object> codec = codecRegistry.codecFor(fieldType);
			value.set(index, resolve(fieldTerm, fieldType, codecRegistry, coordinator, bindings), codec);
		});

		return value;
	}

	/**
	 * A bracket literal is how both a list and a vector are written, so the receiving type decides
	 * which was meant - the parser does not, which is why it produces an {@code ArrayLiteral} rather
	 * than a list. Cassandra names both when the receiver is neither.
	 */
	private static @Nullable Object toListOrVector(final List<Term.Raw> elements,
		final DataType dataType, final CodecRegistry codecRegistry, final Node coordinator,
		final Object... bindings) {
		if (dataType instanceof VectorType vector) {
			if (elements.size() != vector.getDimensions()) {
				throw new InvalidQueryException(coordinator,
					"Invalid vector literal: type %s expects %d elements but got %d".formatted(
						vector.asCql(true, true), vector.getDimensions(), elements.size()));
			}

			return CqlVector.newInstance(
				resolveEach(elements, vector.getElementType(), codecRegistry, coordinator, bindings));
		}
		if (!(dataType instanceof ListType list)) {
			throw new InvalidQueryException(coordinator,
				"Unexpected receiver type '%s'; only list and vector are expected".formatted(
					dataType.asCql(true, true)));
		}

		final var values = resolveEach(elements, list.getElementType(), codecRegistry, coordinator,
			bindings);

		return emptyAsNull(values, values.isEmpty(), list.isFrozen());
	}

	/**
	 * The grammar cannot tell an empty set from an empty map, so it parses {@code {}} as an empty set
	 * literal and leaves the receiving type to say which was meant - exactly what Cassandra's own
	 * {@code Sets.Literal#prepare} does.
	 */
	private static @Nullable Object toSetOrEmptyMap(final List<Term.Raw> elements,
		final DataType dataType, final CodecRegistry codecRegistry, final Node coordinator,
		final Object... bindings) {
		if (dataType instanceof MapType map && elements.isEmpty()) {
			return emptyAsNull(Map.of(), true, map.isFrozen());
		}
		if (!(dataType instanceof SetType set)) {
			throw new InvalidQueryException(coordinator,
				"Invalid set literal for a column of type %s".formatted(dataType.asCql(true, true)));
		}
		final var values = new LinkedHashSet<>(
			resolveEach(elements, set.getElementType(), codecRegistry, coordinator, bindings));

		return emptyAsNull(values, values.isEmpty(), set.isFrozen());
	}

	/**
	 * A map literal is never empty - {@code {}} parses as a set - so the empty case belongs to
	 * {@link #toSetOrEmptyMap}.
	 */
	private static Map<Object, Object> toMap(final Maps.Literal literal, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (!(dataType instanceof MapType map)) {
			throw new InvalidQueryException(coordinator,
				"Invalid map literal for a column of type %s".formatted(dataType.asCql(true, true)));
		}

		final Map<Object, Object> values = new LinkedHashMap<>(literal.entries.size());
		literal.entries.forEach(entry -> values.put(
			resolve(entry.left, map.getKeyType(), codecRegistry, coordinator, bindings),
			resolve(entry.right, map.getValueType(), codecRegistry, coordinator, bindings)));

		return values;
	}

	/**
	 * A tuple literal may name fewer components than its type declares - the rest stay null - but
	 * never more.
	 */
	private static TupleValue toTupleValue(final List<Term.Raw> elements, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		if (!(dataType instanceof TupleType tuple)) {
			throw new InvalidQueryException(coordinator,
				"Invalid tuple literal for a column of type %s".formatted(dataType.asCql(true, true)));
		}
		final var componentTypes = tuple.getComponentTypes();
		if (elements.size() > componentTypes.size()) {
			throw new InvalidQueryException(coordinator,
				"Invalid tuple literal: too many elements. Type %s expects %d but got %d".formatted(
					tuple.asCql(true, true), componentTypes.size(), elements.size()));
		}

		final var value = tuple.newValue();
		for (int i = 0; i < elements.size(); i++) {
			final var componentType = componentTypes.get(i);
			final TypeCodec<Object> codec = codecRegistry.codecFor(componentType);
			value.set(i, resolve(elements.get(i), componentType, codecRegistry, coordinator, bindings),
				codec);
		}

		return value;
	}

	/**
	 * A cast names the type its term is to be read as, and Cassandra requires that type to be the one
	 * the receiver expects. SeaStar resolves the named type with no keyspace to hand, so only the
	 * native types can be cast to.
	 */
	private static @Nullable Object toCast(final TypeCast cast, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		final var raw = FieldBindings.TYPE_CAST_TYPE.require(cast);
		final var term = FieldBindings.TYPE_CAST_TERM.require(cast);
		final var castType = new SeaStarRawType(raw).toNativeDataType()
			.orElseThrow(() -> new UnsupportedOperationException("Unsupported cast to %s".formatted(raw)));
		if (!castType.equals(dataType)) {
			throw new InvalidQueryException(coordinator,
				"Cannot assign value (%s)%s to a column of type %s".formatted(raw, term.getText(),
					dataType.asCql(true, true)));
		}

		return resolve(term, dataType, codecRegistry, coordinator, bindings);
	}

	private static Object call(final FunctionCall.Raw raw, final DataType dataType,
		final CodecRegistry codecRegistry, final Node coordinator) {
		final var name = FieldBindings.FUNCTION_NAME.require(raw).name.toLowerCase(Locale.ROOT);
		final var function = FUNCTIONS.get(name);
		if (function == null) {
			throw new InvalidQueryException(coordinator, "Unknown function %s called".formatted(name));
		}
		final var arguments = FieldBindings.FUNCTION_TERMS.require(raw);
		if (!arguments.isEmpty()) {
			throw new InvalidQueryException(coordinator,
				"Invalid number of arguments in call to function %s: 0 required but found %d".formatted(
					name, arguments.size()));
		}

		final var value = function.get();
		if (!codecRegistry.codecFor(dataType).accepts(value)) {
			throw new InvalidQueryException(coordinator,
				"Type error: cannot assign result of function %s to a column of type %s".formatted(name,
					dataType.asCql(true, true)));
		}

		return value;
	}

	private static List<Object> resolveEach(final List<Term.Raw> terms, final DataType elementType,
		final CodecRegistry codecRegistry, final Node coordinator, final Object... bindings) {
		return terms.stream()
			.map(term -> resolve(term, elementType, codecRegistry, coordinator, bindings))
			.toList();
	}

	/**
	 * An unfrozen collection is stored as one cell per element, so an empty one is no cells at all
	 * and Cassandra reads it back as null. A frozen collection is a single value and stays empty.
	 */
	private static <T> @Nullable T emptyAsNull(final T collection, final boolean empty,
		final boolean frozen) {
		return empty && !frozen ? null : collection;
	}

}
