package com.tagadvance.seastar;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.NonNull;

public final class Maps {

	private Maps() {
		// hidden constructor
	}

	public static <K, V> Map<K, V> alwaysReturn(final V value) {
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
				throw new UnsupportedOperationException("This map is immutable");
			}

			@Override
			public V remove(final Object key) {
				throw new UnsupportedOperationException("This map is immutable");
			}

			@Override
			public void putAll(final @NonNull Map<? extends K, ? extends V> m) {
				throw new UnsupportedOperationException("This map is immutable");
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException("This map is immutable");
			}

			@Override
			@NonNull
			public Set<K> keySet() {
				return Collections.emptySet();
			}

			@Override
			@NonNull
			public Collection<V> values() {
				return Collections.singleton(value);
			}

			@Override
			@NonNull
			public Set<Entry<K, V>> entrySet() {
				return Collections.emptySet();
			}
		});
	}

}
