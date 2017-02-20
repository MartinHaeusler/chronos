package org.chronos.common.collections.immutable.base;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public abstract class AbstractImmutableSet<T> extends AbstractImmutableCollection<T> implements Set<T> {

	// hashcode and equals implementations "borrowed" from "java.util.AbstractSet"

	/**
	 * Compares the specified object with this set for equality. Returns <tt>true</tt> if the given object is also a
	 * set, the two sets have the same size, and every member of the given set is contained in this set. This ensures
	 * that the <tt>equals</tt> method works properly across different implementations of the <tt>Set</tt> interface.
	 * <p>
	 *
	 * This implementation first checks if the specified object is this set; if so it returns <tt>true</tt>. Then, it
	 * checks if the specified object is a set whose size is identical to the size of this set; if not, it returns
	 * false. If so, it returns <tt>containsAll((Collection) o)</tt>.
	 *
	 * @param o
	 *            object to be compared for equality with this set
	 * @return <tt>true</tt> if the specified object is equal to this set
	 */
	@Override
	public boolean equals(final Object o) {
		if (o == this) {
			return true;
		}

		if (!(o instanceof Set)) {
			return false;
		}
		Collection<?> c = (Collection<?>) o;
		if (c.size() != this.size()) {
			return false;
		}
		try {
			return this.containsAll(c);
		} catch (ClassCastException unused) {
			return false;
		} catch (NullPointerException unused) {
			return false;
		}
	}

	/**
	 * Returns the hash code value for this set. The hash code of a set is defined to be the sum of the hash codes of
	 * the elements in the set, where the hash code of a <tt>null</tt> element is defined to be zero. This ensures that
	 * <tt>s1.equals(s2)</tt> implies that <tt>s1.hashCode()==s2.hashCode()</tt> for any two sets <tt>s1</tt> and
	 * <tt>s2</tt>, as required by the general contract of {@link Object#hashCode}.
	 *
	 * <p>
	 * This implementation iterates over the set, calling the <tt>hashCode</tt> method on each element in the set, and
	 * adding up the results.
	 *
	 * @return the hash code value for this set
	 * @see Object#equals(Object)
	 * @see Set#equals(Object)
	 */
	@Override
	public int hashCode() {
		int h = 0;
		Iterator<T> i = this.iterator();
		while (i.hasNext()) {
			T obj = i.next();
			if (obj != null) {
				h += obj.hashCode();
			}
		}
		return h;
	}
}
