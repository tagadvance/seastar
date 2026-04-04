package com.tagadvance.tools;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;

/**
 * Provides utility methods for working with {@link Map maps}.
 */
public final class Maps {

	private Maps() {
		// hidden constructor
	}

	/**
	 * Creates a map that always returns the same value for all keys.
	 *
	 * @param value the value to return for all keys
	 * @param <K>   the type of keys maintained by this map
	 * @param <V>   the type of mapped values
	 * @return a map that always returns the same value for all keys
	 */
	public static <K, V> Map<K, V> alwaysReturn(final V value) {
		return alwaysReturn(value, () -> null);
	}

	/**
	 * Creates a map that always returns the same value for all keys.
	 *
	 * @param value       the value to return for all keys
	 * @param keySupplier the key supplier, may return {@literal null}
	 * @param <K>         the type of keys maintained by this map
	 * @param <V>         the type of mapped values
	 * @return a map that always returns the same value for all keys
	 */
	public static <K, V> Map<K, V> alwaysReturn(@NonNull final V value,
		@NonNull final Supplier<K> keySupplier) {
		requireNonNull(value, "value must not be null");
		requireNonNull(keySupplier, "keySupplier must not be null");

		return Collections.unmodifiableMap(new Map<>() {

			@Override
			public int size() {
				return 1;
			}

			@Override
			public boolean isEmpty() {
				return false;
			}

			@Override
			public boolean containsKey(final Object key) {
				return true;
			}

			@Override
			public boolean containsValue(final Object v) {
				return Objects.equals(v, value);
			}

			@Override
			public V get(final Object key) {
				return value;
			}

			@Override
			public V put(final K key, final V value) {
				throw newImmutableException();
			}

			@Override
			public V remove(final Object key) {
				throw newImmutableException();
			}

			@Override
			public void putAll(final @NonNull Map<? extends K, ? extends V> m) {
				throw newImmutableException();
			}

			@Override
			public void clear() {
				throw newImmutableException();
			}

			@Override
			@NonNull
			public Set<K> keySet() {
				return Optional.of(keySupplier)
					.map(Supplier::get)
					.map(Set::of)
					.orElseGet(Collections::emptySet);
			}

			@Override
			@NonNull
			public Collection<V> values() {
				return Collections.singleton(value);
			}

			@Override
			@NonNull
			public Set<Entry<K, V>> entrySet() {
				return Optional.of(keySupplier).map(Supplier::get).map(key -> new Entry<K, V>() {
					@Override
					public K getKey() {
						return key;
					}

					@Override
					public V getValue() {
						return value;
					}

					@Override
					public V setValue(final V value) {
						throw newImmutableException();
					}
				}).map(Set::<Entry<K, V>>of).orElseGet(Collections::emptySet);
			}

			private static RuntimeException newImmutableException() {
				return new UnsupportedOperationException("This map is immutable");
			}

		});


	}

}
