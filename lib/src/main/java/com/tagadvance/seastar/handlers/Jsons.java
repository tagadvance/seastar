package com.tagadvance.seastar.handlers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

/**
 * CQL's JSON, both directions: the single {@code [json]} column {@code SELECT JSON} returns, and the
 * column values {@code INSERT INTO ... JSON} writes.
 *
 * <p>Cassandra's JSON is not a generic serialization of a row - each CQL type has a defined JSON
 * shape, and a value that reads back correctly through {@code INSERT JSON} is one written in that
 * shape. A blob is the hex string it is written as, a timestamp is
 * {@code "yyyy-MM-dd HH:mm:ss.SSSZ"}, a map's keys are JSON strings whatever the key type is, and a
 * tuple is an array. Those shapes are what this file encodes and decodes; the container suite pins
 * them.
 *
 * <p>Reading is done by turning each JSON value back into the CQL literal it stands for and handing
 * that to the column's codec, so a JSON document and a CQL statement understand a value the same
 * way, and a malformed one fails the same way.
 */
final class Jsons {

	/**
	 * The name of the one column {@code SELECT JSON} returns, brackets and all, exactly as a cluster
	 * reports it.
	 */
	static final CqlIdentifier COLUMN = CqlIdentifier.fromInternal("[json]");

	/**
	 * The types whose JSON form is a string and whose CQL literal form is quoted. A blob is the
	 * exception in both directions: its JSON form is a string, but its CQL literal is bare hex.
	 */
	private static final Set<DataType> QUOTED = Set.of(DataTypes.ASCII, DataTypes.TEXT,
		DataTypes.UUID, DataTypes.TIMEUUID, DataTypes.INET, DataTypes.DATE, DataTypes.TIME,
		DataTypes.TIMESTAMP, DataTypes.DURATION);

	private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern(
		"yyyy-MM-dd HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

	private Jsons() {
		// hidden constructor
	}

	/**
	 * The {@code [json]} text for one result row.
	 *
	 * @param names  the key of each column, in the order the select clause named them
	 * @param types  the type of each column
	 * @param values the value of each column
	 */
	static String encode(final List<CqlIdentifier> names, final List<DataType> types,
		final List<Object> values) {
		final var out = new StringBuilder("{");
		for (int i = 0; i < names.size(); i++) {
			if (i > 0) {
				out.append(", ");
			}
			quote(out, names.get(i).asInternal()).append(": ");
			write(out, values.get(i), types.get(i));
		}

		return out.append('}').toString();
	}

	private static void write(final StringBuilder out, final @Nullable Object value,
		final DataType type) {
		if (value == null) {
			out.append("null");

			return;
		}
		if (value instanceof ByteBuffer bytes) {
			quote(out, hex(bytes));
		} else if (value instanceof Instant instant) {
			quote(out, TIMESTAMP.format(instant));
		} else if (value instanceof InetAddress address) {
			quote(out, address.getHostAddress());
		} else if (value instanceof Boolean || value instanceof Number) {
			out.append(value);
		} else if (type instanceof MapType map) {
			writeMap(out, (Map<?, ?>) value, map);
		} else if (type instanceof ListType list) {
			writeAll(out, (Iterable<?>) value, list.getElementType());
		} else if (type instanceof SetType set) {
			writeAll(out, (Iterable<?>) value, set.getElementType());
		} else if (type instanceof VectorType vector) {
			writeAll(out, (CqlVector<?>) value, vector.getElementType());
		} else if (value instanceof TupleValue tuple) {
			writeTuple(out, tuple);
		} else if (value instanceof UdtValue udt) {
			writeUdt(out, udt);
		} else if (value instanceof CqlDuration duration) {
			quote(out, duration.toString());
		} else {
			quote(out, value.toString());
		}
	}

	private static void writeAll(final StringBuilder out, final Iterable<?> values,
		final DataType elementType) {
		out.append('[');
		var first = true;
		for (final var element : values) {
			if (!first) {
				out.append(", ");
			}
			first = false;
			write(out, element, elementType);
		}
		out.append(']');
	}

	/**
	 * A JSON object's keys are strings, so a map with a non-text key type has its keys rendered as
	 * their CQL form and then quoted - which is what a cluster does, and what makes the document read
	 * back through {@code INSERT JSON}.
	 */
	private static void writeMap(final StringBuilder out, final Map<?, ?> value,
		final MapType type) {
		out.append('{');
		var first = true;
		for (final var entry : value.entrySet()) {
			if (!first) {
				out.append(", ");
			}
			first = false;
			final var key = new StringBuilder();
			write(key, entry.getKey(), type.getKeyType());
			if (key.length() == 0 || key.charAt(0) != '"') {
				quote(out, key.toString());
			} else {
				out.append(key);
			}
			out.append(": ");
			write(out, entry.getValue(), type.getValueType());
		}
		out.append('}');
	}

	private static void writeTuple(final StringBuilder out, final TupleValue value) {
		final var types = value.getType().getComponentTypes();
		out.append('[');
		for (int i = 0; i < types.size(); i++) {
			if (i > 0) {
				out.append(", ");
			}
			write(out, value.getObject(i), types.get(i));
		}
		out.append(']');
	}

	private static void writeUdt(final StringBuilder out, final UdtValue value) {
		final var type = value.getType();
		final var names = type.getFieldNames();
		out.append('{');
		for (int i = 0; i < names.size(); i++) {
			if (i > 0) {
				out.append(", ");
			}
			quote(out, names.get(i).asInternal()).append(": ");
			write(out, value.getObject(i), type.getFieldTypes().get(i));
		}
		out.append('}');
	}

	private static StringBuilder quote(final StringBuilder out, final String text) {
		out.append('"');
		for (int i = 0; i < text.length(); i++) {
			final var c = text.charAt(i);
			switch (c) {
				case '"' -> out.append("\\\"");
				case '\\' -> out.append("\\\\");
				case '\n' -> out.append("\\n");
				case '\r' -> out.append("\\r");
				case '\t' -> out.append("\\t");
				default -> {
					if (c < 0x20) {
						out.append("\\u%04x".formatted((int) c));
					} else {
						out.append(c);
					}
				}
			}
		}

		return out.append('"');
	}

	private static String hex(final ByteBuffer buffer) {
		final var bytes = new byte[buffer.remaining()];
		buffer.duplicate().get(bytes);
		final var out = new StringBuilder(2 + bytes.length * 2).append("0x");
		for (final var value : bytes) {
			out.append("%02x".formatted(value));
		}

		return out.toString();
	}

	/**
	 * The columns an {@code INSERT INTO ... JSON} document names, keyed by column name, with each
	 * value already converted to what the column stores.
	 *
	 * @param columns the column name and type of every column of the table
	 */
	static Map<CqlIdentifier, Object> decode(final String document,
		final Map<CqlIdentifier, DataType> columns, final CodecRegistry codecRegistry,
		final Node coordinator) {
		final Object parsed;
		try {
			parsed = new JsonReader(document).read();
		} catch (final IllegalArgumentException e) {
			throw new InvalidQueryException(coordinator,
				"Could not decode JSON string as a map: %s. (String was: %s)".formatted(e.getMessage(),
					document));
		}
		if (!(parsed instanceof Map<?, ?> object)) {
			throw new InvalidQueryException(coordinator,
				"Expected a map while decoding JSON, but got %s".formatted(document));
		}

		final Map<CqlIdentifier, Object> values = new LinkedHashMap<>();
		object.forEach((key, value) -> {
			final var name = CqlIdentifier.fromInternal(String.valueOf(key));
			final var type = columns.get(name);
			if (type == null) {
				throw new InvalidQueryException(coordinator,
					"JSON values map contains unrecognized column: %s".formatted(name.asInternal()));
			}
			values.put(name, toValue(name, value, type, codecRegistry, coordinator));
		});

		return values;
	}

	private static @Nullable Object toValue(final CqlIdentifier column,
		final @Nullable Object json, final DataType type, final CodecRegistry codecRegistry,
		final Node coordinator) {
		if (json == null) {
			return null;
		}
		try {
			return codecRegistry.codecFor(type).parse(literal(json, type));
		} catch (final IllegalArgumentException e) {
			throw new InvalidQueryException(coordinator,
				"Error decoding JSON value for %s: %s".formatted(column.asInternal(),
					e.getMessage()));
		}
	}

	/**
	 * The CQL literal a JSON value stands for, so that reading a document goes through the same codec
	 * that reads the statement {@code INSERT INTO t (c) VALUES (...)} would have been written as.
	 */
	private static String literal(final @Nullable Object json, final DataType type) {
		if (json == null) {
			return "null";
		}
		if (type instanceof ListType list) {
			return elements(json, list.getElementType(), "[", "]");
		}
		if (type instanceof VectorType vector) {
			return elements(json, vector.getElementType(), "[", "]");
		}
		if (type instanceof SetType set) {
			return elements(json, set.getElementType(), "{", "}");
		}
		if (type instanceof TupleType tuple) {
			return tupleLiteral(json, tuple);
		}
		if (type instanceof MapType map) {
			return mapLiteral(json, map);
		}
		if (type instanceof UserDefinedType udt) {
			return udtLiteral(json, udt);
		}
		if (json instanceof String text) {
			return DataTypes.BLOB.equals(type) ? text : "'" + text.replace("'", "''") + "'";
		}

		return String.valueOf(json);
	}

	private static String elements(final Object json, final DataType elementType,
		final String open, final String close) {
		if (!(json instanceof List<?> list)) {
			throw new IllegalArgumentException("expected a JSON array but got " + json);
		}

		return list.stream()
			.map(element -> literal(element, elementType))
			.collect(Collectors.joining(", ", open, close));
	}

	private static String tupleLiteral(final Object json, final TupleType type) {
		if (!(json instanceof List<?> list)) {
			throw new IllegalArgumentException("expected a JSON array but got " + json);
		}
		final var types = type.getComponentTypes();
		final List<String> parts = new ArrayList<>(list.size());
		for (int i = 0; i < list.size(); i++) {
			parts.add(literal(list.get(i), types.get(Math.min(i, types.size() - 1))));
		}

		return parts.stream().collect(Collectors.joining(", ", "(", ")"));
	}

	private static String mapLiteral(final Object json, final MapType type) {
		if (!(json instanceof Map<?, ?> object)) {
			throw new IllegalArgumentException("expected a JSON object but got " + json);
		}

		return object.entrySet()
			.stream()
			.map(entry -> "%s: %s".formatted(key(String.valueOf(entry.getKey()), type.getKeyType()),
				literal(entry.getValue(), type.getValueType())))
			.collect(Collectors.joining(", ", "{", "}"));
	}

	/**
	 * A map key arrives as a JSON string whatever the key type is, so a numeric key is unquoted again
	 * on the way back into a CQL literal.
	 */
	private static String key(final String text, final DataType type) {
		return QUOTED.contains(type) ? "'" + text.replace("'", "''") + "'" : text;
	}

	private static String udtLiteral(final Object json, final UserDefinedType type) {
		if (!(json instanceof Map<?, ?> object)) {
			throw new IllegalArgumentException("expected a JSON object but got " + json);
		}

		return object.entrySet().stream().map(entry -> {
			final var name = CqlIdentifier.fromInternal(String.valueOf(entry.getKey()));
			final var index = type.firstIndexOf(name);
			if (index < 0) {
				throw new IllegalArgumentException(
					"unknown field %s in %s".formatted(name.asInternal(),
						type.getName().asInternal()));
			}

			return "%s: %s".formatted(name.asCql(true),
				literal(entry.getValue(), type.getFieldTypes().get(index)));
		}).collect(Collectors.joining(", ", "{", "}"));
	}

	/**
	 * A JSON reader small enough to own: SeaStar reads exactly one shape of document, the flat object
	 * an {@code INSERT ... JSON} carries, and pulling in a parser for it would tax every consumer of
	 * the library for a hundred lines of work.
	 */
	private static final class JsonReader {

		private final String text;
		private int position;

		private JsonReader(final String text) {
			this.text = text;
		}

		private Object read() {
			final var value = readValue();
			skipWhitespace();
			if (position < text.length()) {
				throw new IllegalArgumentException(
					"unexpected trailing input at offset " + position);
			}

			return value;
		}

		private @Nullable Object readValue() {
			skipWhitespace();
			if (position >= text.length()) {
				throw new IllegalArgumentException("unexpected end of input");
			}

			return switch (text.charAt(position)) {
				case '{' -> readObject();
				case '[' -> readArray();
				case '"' -> readString();
				case 't' -> readKeyword("true", Boolean.TRUE);
				case 'f' -> readKeyword("false", Boolean.FALSE);
				case 'n' -> readKeyword("null", null);
				default -> readNumber();
			};
		}

		private Map<String, Object> readObject() {
			final Map<String, Object> object = new LinkedHashMap<>();
			position++;
			skipWhitespace();
			if (peek() == '}') {
				position++;

				return object;
			}
			while (true) {
				skipWhitespace();
				final var key = readString();
				skipWhitespace();
				expect(':');
				object.put(key, readValue());
				skipWhitespace();
				if (peek() == ',') {
					position++;
					continue;
				}
				expect('}');

				return object;
			}
		}

		private List<Object> readArray() {
			final List<Object> values = new ArrayList<>();
			position++;
			skipWhitespace();
			if (peek() == ']') {
				position++;

				return values;
			}
			while (true) {
				values.add(readValue());
				skipWhitespace();
				if (peek() == ',') {
					position++;
					continue;
				}
				expect(']');

				return values;
			}
		}

		private String readString() {
			expect('"');
			final var out = new StringBuilder();
			while (true) {
				if (position >= text.length()) {
					throw new IllegalArgumentException("unterminated string");
				}
				final var c = text.charAt(position++);
				if (c == '"') {
					return out.toString();
				}
				if (c != '\\') {
					out.append(c);
					continue;
				}
				final var escape = text.charAt(position++);
				switch (escape) {
					case '"', '\\', '/' -> out.append(escape);
					case 'b' -> out.append('\b');
					case 'f' -> out.append('\f');
					case 'n' -> out.append('\n');
					case 'r' -> out.append('\r');
					case 't' -> out.append('\t');
					case 'u' -> {
						out.append((char) Integer.parseInt(text.substring(position, position + 4), 16));
						position += 4;
					}
					default -> throw new IllegalArgumentException("invalid escape \\" + escape);
				}
			}
		}

		private BigDecimal readNumber() {
			final var start = position;
			while (position < text.length() && "+-.eE0123456789".indexOf(text.charAt(position)) >= 0) {
				position++;
			}
			if (start == position) {
				throw new IllegalArgumentException(
					"unrecognized token at offset %d".formatted(start));
			}
			try {
				return new BigDecimal(text.substring(start, position));
			} catch (final NumberFormatException e) {
				throw new IllegalArgumentException(
					"invalid number %s".formatted(text.substring(start, position)));
			}
		}

		private @Nullable Object readKeyword(final String keyword, final @Nullable Object value) {
			if (!text.startsWith(keyword, position)) {
				throw new IllegalArgumentException(
					"unrecognized token at offset %d".formatted(position));
			}
			position += keyword.length();

			return value;
		}

		private char peek() {
			if (position >= text.length()) {
				throw new IllegalArgumentException("unexpected end of input");
			}

			return text.charAt(position);
		}

		private void expect(final char expected) {
			if (peek() != expected) {
				throw new IllegalArgumentException(
					"expected %c at offset %d but found %c".formatted(expected, position, peek()));
			}
			position++;
		}

		private void skipWhitespace() {
			while (position < text.length() && Character.isWhitespace(text.charAt(position))) {
				position++;
			}
		}

	}

}
