package com.tagadvance.seastar.handlers;

import java.util.Optional;

final class Reflections {

	private Reflections() {
		// hidden constructor
	}

	@SuppressWarnings("unchecked")
	static <V> Optional<V> getDeclaredField(final Object o, final String name,
		final Class<V> returnType) {
		try {
			final var field = o.getClass().getDeclaredField(name);
			field.setAccessible(true);
			final var value = field.get(o);
			if (returnType.isInstance(value)) {
				return Optional.of((V) value);
			} else if (value instanceof Optional<?> optional && returnType.isInstance(
				optional.orElse(null))) {
				return (Optional<V>) optional;
			}
		} catch (final NoSuchFieldException | IllegalAccessException e) {
			throw new ReflectionException(
				"Failed to get field %s from object of type %s".formatted(name,
					o.getClass().getSimpleName()), e);
		}

		return Optional.empty();
	}

}
