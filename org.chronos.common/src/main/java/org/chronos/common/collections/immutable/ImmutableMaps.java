package org.chronos.common.collections.immutable;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.chronos.common.collections.immutable.base.AbstractImmutableSet;
import org.chronos.common.collections.immutable.hamt.HashArrayMappedTree;

public class ImmutableMaps {

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public static <K, V> Map<K, V> asUnmodifiableMap(final ImmutableMap<K, V> immutableMap) {
		if (immutableMap == null) {
			throw new NullPointerException("Precondition violation - argument 'immutableMap' must not be NULL!");
		}
		return new UnmodifiableMapView<K, V>(immutableMap);
	}

	public static <K, V> ImmutableMap<K, V> newHashArrayMappedTreeMap() {
		return HashArrayMappedTree.create();
	}

	public static <K, V> ImmutableMap<K, V> newHashArrayMappedTreeMap(final Map<K, V> map) {
		if (map == null) {
			throw new NullPointerException("Precondition violation - argument 'map' must not be NULL!");
		}
		return HashArrayMappedTree.create(map);
	}

	public static <K, V> ImmutableMap<K, V> newHashArrayMappedTreeMap(final Iterable<Entry<K, V>> entries) {
		if (entries == null) {
			throw new NullPointerException("Precondition violation - argument 'entries' must not be NULL!");
		}
		return HashArrayMappedTree.create(entries);
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static class UnmodifiableMapView<K, V> extends AbstractMap<K, V> {

		private final ImmutableMap<K, V> map;

		public UnmodifiableMapView(final ImmutableMap<K, V> map) {
			if (map == null) {
				throw new NullPointerException("Precondition violation - argument 'map' must not be NULL!");
			}
			this.map = map;
		}

		@Override
		public int size() {
			return this.map.size();
		}

		@Override
		public boolean isEmpty() {
			return this.map.isEmpty();
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean containsKey(final Object key) {
			return this.map.containsKey((K) key);
		}

		@Override
		public boolean containsValue(final Object value) {
			return this.map.values().contains(value);
		}

		@Override
		@SuppressWarnings("unchecked")
		public V get(final Object key) {
			return this.map.get((K) key);
		}

		@Override
		public V put(final K key, final V value) {
			throw new UnsupportedOperationException("UnmodifiableMapView does not support changing the contents!");
		}

		@Override
		public V remove(final Object key) {
			throw new UnsupportedOperationException("UnmodifiableMapView does not support changing the contents!");
		}

		@Override
		public void putAll(final Map<? extends K, ? extends V> m) {
			throw new UnsupportedOperationException("UnmodifiableMapView does not support changing the contents!");
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException("UnmodifiableMapView does not support changing the contents!");
		}

		@Override
		public Set<K> keySet() {
			return this.map.keySet();
		}

		@Override
		public Collection<V> values() {
			return this.map.values();
		}

		@Override
		public Set<java.util.Map.Entry<K, V>> entrySet() {
			return new EntrySetView();
		}

		private class EntrySetView extends AbstractImmutableSet<Entry<K, V>> {

			@Override
			public int size() {
				return UnmodifiableMapView.this.size();
			}

			@Override
			@SuppressWarnings("unchecked")
			public boolean contains(final Object o) {
				if (o instanceof Entry == false) {
					return false;
				}
				Entry<K, V> entry = (Entry<K, V>) o;
				return UnmodifiableMapView.this.map.containsEntry(entry);
			}

			@Override
			public Iterator<Entry<K, V>> iterator() {
				return UnmodifiableMapView.this.map.iterator();
			}

		}
	}
}
