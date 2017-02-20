package org.chronos.chronograph.internal.impl.util;

/**
 * A simple helper class that wraps an element, and implements {@link #hashCode()} and {@link #equals(Object)} based on
 * object identity.
 * 
 * <p>
 * The methods are implemented as follows:
 * <ul>
 * <li><b><code>hashCode()</code></b>: Uses {@link System#identityHashCode(Object)} on the wrapped object
 * <li><b><code>equals(other)</code></b>: Uses the <code>==</code> operator between the wrapped object in
 * <code>this</code> and <code>other</code>
 * </ul>
 * 
 * This class is useful for cases where object identity should <b>not</b> be determined by the individual
 * {@link #hashCode()} and {@link #equals(Object)} overrides of the wrapped object.
 * 
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <T>
 *            The type of element contained in this wrapper
 */
public class IdentityWrapper<T> {

	public static <T> IdentityWrapper<T> of(final T element) {
		return new IdentityWrapper<T>(element);
	}

	private final T contents;

	private Integer hashCode = null;

	public IdentityWrapper(final T element) {
		if (element == null) {
			throw new IllegalArgumentException("Precondition violation - argument 'element' must not be NULL!");
		}
		this.contents = element;
	}

	public T get() {
		return this.contents;
	}

	@Override
	public int hashCode() {
		if (this.hashCode == null) {
			this.hashCode = System.identityHashCode(this.contents);
		}
		return this.hashCode;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj instanceof IdentityWrapper == false) {
			return false;
		}
		IdentityWrapper<?> other = (IdentityWrapper<?>) obj;
		return this.contents == other.contents;
	}

	@Override
	public String toString() {
		return "IdentityWrapper[" + this.contents.toString() + "]";
	}

}