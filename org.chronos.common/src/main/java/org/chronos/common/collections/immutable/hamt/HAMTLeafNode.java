package org.chronos.common.collections.immutable.hamt;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.chronos.common.collections.immutable.base.ImmutableEntry;

public class HAMTLeafNode<K, V> implements HAMTNode<K, V> {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private Entry<K, V>[] entries;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public HAMTLeafNode() {
		this.entries = null;
	}

	@SuppressWarnings("unchecked")
	public HAMTLeafNode(final Entry<K, V> entry) {
		this.entries = new Entry[1];
		this.entries[0] = ImmutableEntry.of(entry);
	}

	public HAMTLeafNode(final K key, final V value) {
		this(ImmutableEntry.of(key, value));
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public boolean isLeaf() {
		return true;
	}

	@Override
	public boolean containsKey(final K key) {
		if (key == null || this.entries == null || this.entries.length <= 0) {
			return false;
		}
		for (Entry<K, V> entry : this.entries) {
			if (entry.getKey().equals(key)) {
				return true;
			}
		}
		return false;
	}

	public V getValueForKey(final K key) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		if (this.entries == null || this.entries.length <= 0) {
			return null;
		}
		for (Entry<K, V> entry : this.entries) {
			if (entry.getKey().equals(key)) {
				return entry.getValue();
			}
		}
		return null;
	}

	@Override
	public int entryCount() {
		if (this.entries == null) {
			return 0;
		}
		return this.entries.length;
	}

	@Override
	public int childNodeCount() {
		return 0;
	}

	public Entry<K, V> getSingleEntry() {
		if (this.entries == null || this.entries.length <= 0) {
			throw new NoSuchElementException("Entries array is empty!");
		}
		if (this.entries.length > 1) {
			throw new IllegalStateException("Entries array has more than one member, cannot get single entry!");
		}
		return this.entries[0];
	}

	public HAMTLeafNode<K, V> putEntry(final Entry<K, V> entry) {
		Entry<K, V>[] newEntries = putEntryIntoUniqueArray(this.entries, entry);
		if (newEntries == this.entries) {
			// no change; this can happen if the entry was already contained. No need to change the node
			return this;
		}
		HAMTLeafNode<K, V> copy = new HAMTLeafNode<>();
		copy.entries = newEntries;
		return copy;
	}

	public HAMTLeafNode<K, V> removeEntry(final K key) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		Entry<K, V>[] newEntries = removeEntryFromUniqueArray(this.entries, key);
		if (newEntries == this.entries) {
			// array is unchanged; this can happen if the key was not contained. No need to change the node
			return this;
		}
		HAMTLeafNode<K, V> copy = new HAMTLeafNode<>();
		copy.entries = newEntries;
		return copy;
	}

	@Override
	public Iterator<Entry<K, V>> entriesIterator() {
		if (this.entries == null || this.entries.length <= 0) {
			return Collections.emptyIterator();
		} else {
			return new EntriesIterator();
		}
	}

	@Override
	public Iterator<HAMTNode<K, V>> childNodeIterator() {
		return Collections.emptyIterator();
	}

	@Override
	public int treeHeight() {
		return 1;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("NL");
		builder.append("[");
		if (this.entries != null) {
			String separator = "";
			for (Entry<K, V> entry : this.entries) {
				builder.append(separator);
				separator = ",";
				builder.append(entry);
			}
		}
		builder.append("]");
		return builder.toString();
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	@SuppressWarnings("unchecked")
	private static <K, V> Entry<K, V>[] putEntryIntoUniqueArray(final Entry<K, V>[] array, final Entry<K, V> entry) {
		if (array == null) {
			Entry<K, V>[] resultArray = new Entry[1];
			resultArray[0] = ImmutableEntry.of(entry);
			return resultArray;
		}
		for (int i = 0; i < array.length; i++) {
			Entry<K, V> existingEntry = array[i];
			if (existingEntry.getKey().equals(entry.getKey())) {
				if (Objects.equals(existingEntry.getValue(), entry.getValue())) {
					// nothing to do
					return array;
				} else {
					// need to replace the entry
					Entry<K, V>[] resultArray = Arrays.copyOf(array, array.length);
					resultArray[i] = ImmutableEntry.of(entry);
					return resultArray;
				}
			}
		}
		// no matching key was found, add it to the end of the array
		Entry<K, V>[] resultArray = new Entry[array.length + 1];
		System.arraycopy(array, 0, resultArray, 0, array.length);
		resultArray[array.length] = ImmutableEntry.of(entry);
		return resultArray;
	}

	@SuppressWarnings("unchecked")
	private static <K, V> Entry<K, V>[] removeEntryFromUniqueArray(final Entry<K, V>[] array, final K key) {
		if (key == null) {
			throw new NullPointerException("Precondition violation - argument 'key' must not be NULL!");
		}
		if (array == null || array.length <= 0) {
			// an empty array can never contain anything, in particular not the given key; nothing to do
			return array;
		}
		int indexToRemove = -1;
		for (int i = 0; i < array.length; i++) {
			Entry<K, V> entry = array[i];
			if (entry.getKey().equals(key)) {
				indexToRemove = i;
				break;
			}
		}
		if (indexToRemove < 0) {
			// entry to remove not found; nothing to do
			return array;
		}
		if (array.length == 1 && indexToRemove == 0) {
			// removing the single entry from a one-element array effectively deletes the array
			return null;
		}
		// create the new array
		Entry<K, V>[] newArray = new Entry[array.length - 1];
		// copy everything, except the index we want to remove
		System.arraycopy(array, 0, newArray, 0, indexToRemove);
		System.arraycopy(array, indexToRemove + 1, newArray, indexToRemove, array.length - indexToRemove - 1);
		// return the new array
		return newArray;
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class EntriesIterator implements Iterator<Entry<K, V>> {

		private int index;
		private int length;

		public EntriesIterator() {
			this.index = 0;
			this.length = HAMTLeafNode.this.entries.length;
		}

		@Override
		public boolean hasNext() {
			return this.index < this.length;
		}

		@Override
		public Entry<K, V> next() {
			if (this.index >= this.length) {
				throw new NoSuchElementException("Iterator exhausted, there are no more elements.");
			}
			Entry<K, V> element = HAMTLeafNode.this.entries[this.index];
			this.index++;
			return element;
		}

	}

}
