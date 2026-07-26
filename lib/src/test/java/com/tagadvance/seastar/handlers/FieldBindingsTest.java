package com.tagadvance.seastar.handlers;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The gate on a cassandra-all upgrade.
 *
 * <p>SeaStar reads state out of Cassandra's parse tree that has no public accessor. A release that
 * renames one of those fields would otherwise change no code, break no compile, and simply make
 * SeaStar answer wrong. Walking the binding table turns that into a red build on the day the
 * dependency is bumped.
 */
class FieldBindingsTest {

	@Test
	@DisplayName("every reflected cassandra-all field still resolves with its expected type")
	void resolves() {
		final var bindings = assertDoesNotThrow(FieldBindingsTest::declaredBindings,
			"FieldBindings did not initialize against cassandra-all " + Reflections.CASSANDRA_VERSION);
		assertFalse(bindings.isEmpty(), "no bindings found; FieldBindings should not be empty");

		assertAll(bindings.stream().map(entry -> () -> {
			final var binding = entry.getValue();
			assertTrue(Reflections.findDeclaredField(binding.owner(), binding.name(), binding.type())
					.isPresent(),
				"FieldBindings.%s no longer resolves: %s.%s of type %s is absent from cassandra-all %s".formatted(
					entry.getKey(), binding.owner().getName(), binding.name(),
					binding.type().getName(), Reflections.CASSANDRA_VERSION));
		}));
	}

	@Test
	@DisplayName("the cassandra-all version is known, so a binding failure can name it")
	void version() {
		assertNotEquals("unknown", Reflections.CASSANDRA_VERSION);
	}

	/**
	 * Reading a static field forces {@code FieldBindings} to initialize, which is where every
	 * binding is resolved, so a rename fails here before any assertion runs.
	 */
	private static List<Map.Entry<String, FieldBinding<?>>> declaredBindings() throws Exception {
		final var fields = Arrays.stream(FieldBindings.class.getDeclaredFields())
			.filter(field -> Modifier.isStatic(field.getModifiers()))
			.filter(field -> field.getType() == FieldBinding.class)
			.toList();

		final List<Map.Entry<String, FieldBinding<?>>> bindings = new ArrayList<>(
			fields.size());
		for (final var field : fields) {
			field.setAccessible(true);
			bindings.add(Map.entry(field.getName(), (FieldBinding<?>) field.get(null)));
		}

		return bindings;
	}

}
