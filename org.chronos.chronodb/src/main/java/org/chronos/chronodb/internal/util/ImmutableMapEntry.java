package org.chronos.chronodb.internal.util;

import java.util.Map;

public class ImmutableMapEntry<K, V> implements Map.Entry<K, V> {

	public static <K, V> ImmutableMapEntry<K, V> create(final K key, final V value) {
		return new ImmutableMapEntry<K, V>(key, value);
	}

	private final K key;
	private final V value;

	public ImmutableMapEntry(final K key, final V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public K getKey() {
		return this.key;
	}

	@Override
	public V getValue() {
		return this.value;
	}

	@Override
	public V setValue(final V value) {
		throw new UnsupportedOperationException("setValue() is not supported on an ImmutableMapEntry!");
	}

}