package com.tagadvance.seastar.handlers;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A single (declaring class, field name, expected type) triple, resolved once when
 * {@link FieldBindings} initializes. Handlers ask a binding for a value instead of passing string
 * literals at the call site, so the surface that a cassandra-all upgrade can break is one file wide.
 *
 * <p>{@link #require(Object)} is for state a statement always carries; {@link #find(Object)} is for
 * state that is genuinely optional in the CQL (an unnamed index, a condition that is not an IN).
 * Note the distinction is about the <em>value</em>: the field itself must exist either way, or the
 * binding fails to resolve at class-init.
 *
 * @param <T> the type the field holds
 */
final class FieldBinding<T> {

	private final Class<?> owner;
	private final String name;
	private final Class<?> type;
	private final Field field;

	private FieldBinding(final Class<?> owner, final String name, final Class<?> type) {
		this.owner = owner;
		this.name = name;
		this.type = type;
		this.field = Reflections.requireDeclaredField(owner, name, type);
	}

	static <T> FieldBinding<T> of(final Class<?> owner, final String name, final Class<T> type) {
		return new FieldBinding<>(owner, name, type);
	}

	/**
	 * Binds a field whose declared type is an enum SeaStar cannot name, because the enum itself is
	 * package-private.
	 */
	@SuppressWarnings("unchecked")
	static FieldBinding<Enum<?>> ofEnum(final Class<?> owner, final String name) {
		return (FieldBinding<Enum<?>>) (FieldBinding<?>) of(owner, name, Enum.class);
	}

	/**
	 * Binds a {@code List} field. Erasure means only the raw type can be verified, so the element
	 * type is the caller's assertion; it is stated once here rather than at every call site.
	 */
	@SuppressWarnings("unchecked")
	static <E> FieldBinding<List<E>> ofList(final Class<?> owner, final String name) {
		return (FieldBinding<List<E>>) (FieldBinding<?>) of(owner, name, List.class);
	}

	/**
	 * Binds a {@code Map} field. See {@link #ofList(Class, String)} on element types.
	 */
	@SuppressWarnings("unchecked")
	static <K, V> FieldBinding<Map<K, V>> ofMap(final Class<?> owner, final String name) {
		return (FieldBinding<Map<K, V>>) (FieldBinding<?>) of(owner, name, Map.class);
	}

	/**
	 * Binds a {@code Set} field. See {@link #ofList(Class, String)} on element types.
	 */
	@SuppressWarnings("unchecked")
	static <E> FieldBinding<Set<E>> ofSet(final Class<?> owner, final String name) {
		return (FieldBinding<Set<E>>) (FieldBinding<?>) of(owner, name, Set.class);
	}

	Class<?> owner() {
		return owner;
	}

	String name() {
		return name;
	}

	Class<?> type() {
		return type;
	}

	/**
	 * Reads a value the statement always carries.
	 *
	 * @throws IllegalStateException if the value is null, which means SeaStar asked the wrong
	 *                               statement shape rather than that cassandra-all changed
	 */
	T require(final Object target) {
		final var value = Reflections.read(field, target);
		if (value == null) {
			throw new IllegalStateException(
				"field %s of %s is required but was null".formatted(name, owner.getName()));
		}

		return cast(value);
	}

	/**
	 * Reads a value that may legitimately be absent.
	 */
	Optional<T> find(final Object target) {
		return Optional.ofNullable(Reflections.read(field, target)).map(this::cast);
	}

	@SuppressWarnings("unchecked")
	private T cast(final Object value) {
		return (T) type.cast(value);
	}

}
