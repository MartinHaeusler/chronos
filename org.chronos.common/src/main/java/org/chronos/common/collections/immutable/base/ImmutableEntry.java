package org.chronos.common.collections.immutable.base;

import java.util.Map.Entry;

public class ImmutableEntry<K, V> implements Entry<K, V> {

	public static <K, V> ImmutableEntry<K, V> of(final K key, final V value) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		return new ImmutableEntry<K, V>(key, value);
	}

	public static <K, V> ImmutableEntry<K, V> of(final Entry<K, V> entry) {
		if (entry == null) {
			throw new NullPointerException("Precondition violation - argument 'entry' must not be NULL!");
		}
		if (entry.getKey() == null) {
			throw new IllegalArgumentException(
					"Precondition violation - the 'key' of the 'entry' parameter must not be NULL!");
		}
		if (entry instanceof ImmutableEntry) {
			return (ImmutableEntry<K, V>) entry;
		} else {
			return of(entry.getKey(), entry.getValue());
		}
	}

	private K key;
	private V value;

	protected ImmutableEntry() {
		// default constructor for serialization
	}

	protected ImmutableEntry(final K key, final V value) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
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
		throw new UnsupportedOperationException("ImmutableEntry does not support 'setValue(...)'!");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.key == null ? 0 : this.key.hashCode());
		result = prime * result + (this.value == null ? 0 : this.value.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		@SuppressWarnings("unchecked")
		ImmutableEntry<K, V> other = (ImmutableEntry<K, V>) obj;
		if (this.key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!this.key.equals(other.key)) {
			return false;
		}
		if (this.value == null) {
			if (other.value != null) {
				return false;
			}
		} else if (!this.value.equals(other.value)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "E[" + this.key + "->" + this.value + "]";
	}

}