package com.tagadvance.seastar.handlers;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Reflections {

	private static final Logger logger = LoggerFactory.getLogger(Reflections.class);

	private Reflections() {
		// hidden constructor
	}

	@SuppressWarnings("unchecked")
	static <V> Optional<V> getDeclaredField(final Object o, final String name,
		final Class<V> returnType) {
		final var objectClass = o.getClass();
		try {
			final var field = objectClass.getDeclaredField(name);
			field.setAccessible(true);
			final var value = field.get(o);
			if (returnType.isInstance(value)) {
				return Optional.of((V) value);
			} else if (value instanceof Optional<?> optional && returnType.isInstance(
				optional.orElse(null))) {
				return (Optional<V>) optional;
			}
		} catch (final NoSuchFieldException e) {
			logger.error(e.getMessage(), e);
		} catch (final IllegalAccessException e) {
			throw new ReflectionException(
				"Failed to get field %s from object of type %s".formatted(name,
					objectClass.getName()), e);
		}

		return Optional.empty();
	}

}
