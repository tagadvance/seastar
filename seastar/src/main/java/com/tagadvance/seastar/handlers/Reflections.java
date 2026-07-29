package com.tagadvance.seastar.handlers;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import org.apache.cassandra.cql3.CQLStatement;

/**
 * Low-level access to the package-private state of Cassandra's parse tree.
 *
 * <p>Two lookups, deliberately distinct. {@link #requireDeclaredField(Class, String, Class)} is for
 * state SeaStar cannot work without: a miss is a {@link ReflectionException} naming the field, the
 * class and the cassandra-all version, because silently defaulting a renamed field turns an upgrade
 * into a wrong answer with no error. {@link #findDeclaredField(Class, String, Class)} is silent, for
 * the genuine probes where absence really is a valid answer.
 *
 * <p>Callers do not use this directly; every (class, field, type) triple SeaStar depends on lives in
 * {@link FieldBindings}, which resolves them once at class-init.
 */
final class Reflections {

	/**
	 * The cassandra-all version whose field names the bindings were written against, so a failure
	 * says which jar disagreed rather than only which field was missing.
	 */
	static final String CASSANDRA_VERSION = Optional.ofNullable(
			CQLStatement.class.getPackage().getImplementationVersion())
		.orElse("unknown");

	private static final Map<Class<?>, Class<?>> BOXED = Map.of(boolean.class, Boolean.class,
		byte.class, Byte.class, char.class, Character.class, short.class, Short.class, int.class,
		Integer.class, long.class, Long.class, float.class, Float.class, double.class, Double.class);

	private Reflections() {
		// hidden constructor
	}

	/**
	 * Resolves a field SeaStar cannot work without.
	 *
	 * @throws ReflectionException if no such field exists or it does not hold {@code type}
	 */
	static Field requireDeclaredField(final Class<?> owner, final String name,
		final Class<?> type) {
		return findDeclaredField(owner, name, type).orElseThrow(() -> new ReflectionException(
			"No field %s of type %s on %s in cassandra-all %s".formatted(name, type.getName(),
				owner.getName(), CASSANDRA_VERSION)));
	}

	/**
	 * Resolves a field that may legitimately be absent, without logging or throwing.
	 */
	static Optional<Field> findDeclaredField(final Class<?> owner, final String name,
		final Class<?> type) {
		// Walk the hierarchy so a field declared on a superclass (e.g. ifExists) still resolves.
		for (Class<?> c = owner; c != null; c = c.getSuperclass()) {
			try {
				final var field = c.getDeclaredField(name);
				if (!type.isAssignableFrom(boxed(field.getType()))) {
					return Optional.empty();
				}
				field.setAccessible(true);

				return Optional.of(field);
			} catch (final NoSuchFieldException e) {
				// declared further up the hierarchy, if at all
			}
		}

		return Optional.empty();
	}

	/**
	 * Resolves a Cassandra class that is package-private and so cannot be named in source.
	 *
	 * @throws ReflectionException if the class is not on the classpath
	 */
	static Class<?> requireClass(final String name) {
		try {
			return Class.forName(name);
		} catch (final ClassNotFoundException e) {
			throw new ReflectionException(
				"No class %s in cassandra-all %s".formatted(name, CASSANDRA_VERSION), e);
		}
	}

	static Object read(final Field field, final Object target) {
		try {
			return field.get(target);
		} catch (final IllegalAccessException | IllegalArgumentException e) {
			throw new ReflectionException(
				"Failed to read field %s of %s from %s in cassandra-all %s".formatted(field.getName(),
					field.getDeclaringClass().getName(), target.getClass().getName(),
					CASSANDRA_VERSION), e);
		}
	}

	private static Class<?> boxed(final Class<?> type) {
		return BOXED.getOrDefault(type, type);
	}

}
