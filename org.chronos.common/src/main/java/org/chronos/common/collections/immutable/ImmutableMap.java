package org.chronos.common.collections.immutable;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Objects;

public interface ImmutableMap<K, V> extends Iterable<Entry<K, V>> {

	public V get(K key);

	public boolean containsKey(K key);

	public ImmutableMap<K, V> put(K key, V value);

	public ImmutableMap<K, V> putAll(Map<K, V> map);

	public ImmutableMap<K, V> putAll(ImmutableMap<K, V> map);

	public ImmutableMap<K, V> remove(K key);

	public ImmutableMap<K, V> remove(K key, V value);

	public ImmutableMap<K, V> remove(Entry<K, V> entry);

	public ImmutableMap<K, V> removeAll(Map<K, V> map);

	public ImmutableMap<K, V> removeAll(ImmutableMap<K, V> map);

	public ImmutableMap<K, V> removeAll(Collection<K> keys);

	public int size();

	public Set<K> keySet();

	public Collection<V> values();

	public boolean isEmpty();

	public default Set<Entry<K, V>> entrySet() {
		return this.asUnmodifiableMap().entrySet();
	}

	public default boolean containsEntry(final K key, final V value) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		V containedValue = this.get(key);
		if (containedValue == null) {
			// not contained (NULL values are not allowed)
			return false;
		}
		return Objects.equal(containedValue, value);
	}

	public default boolean containsEntry(final Entry<K, V> entry) {
		if (entry == null) {
			throw new NullPointerException("Precondition violation - argument 'entry' must not be NULL!");
		}
		return this.containsEntry(entry.getKey(), entry.getValue());
	}

	public default boolean containsValue(final V value) {
		return this.values().contains(value);
	}

	public default Map<K, V> asUnmodifiableMap() {
		return ImmutableMaps.asUnmodifiableMap(this);
	}

	public default Stream<Entry<K, V>> stream() {
		return StreamSupport.stream(this.spliterator(), false);
	}

	public default Stream<Entry<K, V>> parallelStream() {
		return StreamSupport.stream(this.spliterator(), true);
	}
}
