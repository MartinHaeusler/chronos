package org.chronos.common.collections.immutable.base;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

public abstract class AbstractImmutableCollection<T> implements Collection<T> {

	@Override
	public boolean isEmpty() {
		return this.size() <= 0;
	}

	@Override
	public Object[] toArray() {
		Object[] array = new Object[this.size()];
		this.fillArray(array);
		return array;
	}

	@Override
	@SuppressWarnings({ "unchecked", "hiding" })
	public <T> T[] toArray(final T[] a) {
		T[] array = a;
		// make sure that the array is big enough, create a new one if it is too small
		if (a == null || a.length < this.size()) {
			Class<T> clazz = (Class<T>) a.getClass().getComponentType();
			array = (T[]) Array.newInstance(clazz, this.size());
		}
		this.fillArray(array);
		return array;
	}

	private void fillArray(final Object[] array) {
		int index = 0;
		for (T element : this) {
			array[index] = element;
			index++;
		}
	}

	@Override
	public boolean containsAll(final Collection<?> c) {
		if (c == null) {
			throw new NullPointerException("Precondition violation - argument 'c' must not be NULL!");
		}
		for (Object element : c) {
			if (this.contains(element) == false) {
				return false;
			}
		}
		return true;
	}

	// toString implementation "borrowed" from "java.util.AbstractCollection"

	/**
	 * Returns a string representation of this collection. The string representation consists of a list of the
	 * collection's elements in the order they are returned by its iterator, enclosed in square brackets (<tt>"[]"</tt>
	 * ). Adjacent elements are separated by the characters <tt>", "</tt> (comma and space). Elements are converted to
	 * strings as by {@link String#valueOf(Object)}.
	 *
	 * @return a string representation of this collection
	 */
	@Override
	public String toString() {
		Iterator<T> it = this.iterator();
		if (!it.hasNext()) {
			return "[]";
		}

		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for (;;) {
			T e = it.next();
			sb.append(e == this ? "(this Collection)" : e);
			if (!it.hasNext()) {
				return sb.append(']').toString();
			}
			sb.append(',').append(' ');
		}
	}

	// =====================================================================================================================
	// UNSUPPORTED OPERATIONS
	// =====================================================================================================================

	@Override
	public boolean add(final T e) {
		throw new UnsupportedOperationException("This collection is immutable, modifications are not supported!");
	}

	@Override
	public boolean remove(final Object o) {
		throw new UnsupportedOperationException("This collection is immutable, modifications are not supported!");
	}

	@Override
	public boolean addAll(final Collection<? extends T> c) {
		throw new UnsupportedOperationException("This collection is immutable, modifications are not supported!");
	}

	@Override
	public boolean removeAll(final Collection<?> c) {
		throw new UnsupportedOperationException("This collection is immutable, modifications are not supported!");
	}

	@Override
	public boolean retainAll(final Collection<?> c) {
		throw new UnsupportedOperationException("This collection is immutable, modifications are not supported!");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("This collection is immutable, modifications are not supported!");
	}

}
