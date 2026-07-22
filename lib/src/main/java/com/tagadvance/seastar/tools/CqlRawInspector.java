package com.tagadvance.seastar.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;

/**
 * Standalone utility that parses a CQL string with Cassandra's {@link QueryProcessor} and prints the
 * fully qualified {@link CQLStatement.Raw} class, followed by a bounded reflective dump of its
 * package-private fields. The class name can be fed to {@code javap -p}; the field dump shows the
 * concrete field names and values a {@code CqlHandler} would read via {@code Reflections}.
 *
 * <p>Usage: {@code ./gradlew :lib:inspectRaw -Pquery="CREATE KEYSPACE foo ..."}
 */
public final class CqlRawInspector {

	private static final int MAX_DEPTH = 6;
	private static final String RECURSE_PREFIX = "org.apache.cassandra";

	private CqlRawInspector() {
	}

	public static void main(final String[] args) {
		final var query = String.join(" ", args).trim();
		if (query.isEmpty()) {
			System.err.println("usage: CqlRawInspector <cql query>");
			System.exit(2);
		}

		final CQLStatement.Raw raw = QueryProcessor.parseStatement(query);

		final var out = new StringBuilder();
		out.append("query: ").append(query).append('\n');
		out.append("class: ").append(raw.getClass().getName()).append('\n');
		dump(out, raw, 0, Collections.newSetFromMap(new IdentityHashMap<>()));
		System.out.println(out);
	}

	private static void dump(final StringBuilder out, final Object obj, final int depth,
		final Set<Object> seen) {
		for (Class<?> c = obj.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
			for (final Field field : c.getDeclaredFields()) {
				if (Modifier.isStatic(field.getModifiers())) {
					continue;
				}
				final Object value;
				try {
					field.setAccessible(true);
					value = field.get(obj);
				} catch (final ReflectiveOperationException | RuntimeException e) {
					indent(out, depth).append(field.getName()).append(" = <").append(e).append(">\n");
					continue;
				}
				indent(out, depth).append(field.getType().getSimpleName()).append(' ')
					.append(field.getName()).append(" = ").append(describe(value)).append('\n');
				recurse(out, value, depth, seen);
			}
		}
	}

	private static void recurse(final StringBuilder out, final Object value, final int depth,
		final Set<Object> seen) {
		if (value == null || depth >= MAX_DEPTH || !seen.add(value)) {
			return;
		}
		if (value instanceof Collection<?> collection) {
			int i = 0;
			for (final Object element : collection) {
				if (shouldRecurse(element)) {
					indent(out, depth + 1).append('[').append(i).append("] ")
						.append(element.getClass().getName()).append('\n');
					dump(out, element, depth + 2, seen);
				}
				i++;
			}
		} else if (value instanceof Map<?, ?> map) {
			map.forEach((k, v) -> {
				indent(out, depth + 1).append("key ").append(describe(k)).append('\n');
				if (shouldRecurse(v)) {
					dump(out, v, depth + 2, seen);
				}
			});
		} else if (shouldRecurse(value)) {
			dump(out, value, depth + 1, seen);
		}
	}

	private static boolean shouldRecurse(final Object value) {
		if (value == null || value instanceof Enum<?>) {
			return false;
		}
		final var name = value.getClass().getName();
		return name.startsWith(RECURSE_PREFIX) && !name.startsWith("org.apache.cassandra.db.marshal")
			&& !name.contains("$$Lambda");
	}

	private static String describe(final Object value) {
		if (value == null) {
			return "null";
		}
		final var type = value.getClass().getName();
		final String text;
		try {
			text = String.valueOf(value);
		} catch (final RuntimeException e) {
			return type + " <toString failed: " + e + ">";
		}
		return type.startsWith(RECURSE_PREFIX) ? type + " {" + text + "}" : text;
	}

	private static StringBuilder indent(final StringBuilder out, final int depth) {
		return out.append("  ".repeat(depth + 1));
	}

}
