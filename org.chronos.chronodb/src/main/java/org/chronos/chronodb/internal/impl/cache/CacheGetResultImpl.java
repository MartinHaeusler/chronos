package org.chronos.chronodb.internal.impl.cache;

import org.chronos.chronodb.api.exceptions.CacheGetResultNotPresentException;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;

/**
 * A straight-forward implementation of {@link CacheGetResult}.
 *
 * <p>
 * Instead of using the constructor, please make use of the static factory methods provided by this class.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <T>
 *            The return type of the {@link #getValue()} operation.
 */
public class CacheGetResultImpl<T> implements CacheGetResult<T> {

	// =====================================================================================================================
	// SINGLETON PART
	// =====================================================================================================================

	/** This is the singleton instance of this class representing a cache miss. */
	private static final CacheGetResultImpl<?> MISS;

	static {
		MISS = new CacheGetResultImpl<>(false, null);
	}

	// =====================================================================================================================
	// FACTORY METHODS
	// =====================================================================================================================

	/**
	 * Returns a new instance of this class representing a cache hit with the given value.
	 *
	 * @param value
	 *            The value to use as the result of {@link #getValue()} in the new instance.
	 *
	 * @return The new instance. Never <code>null</code>.
	 */
	public static <T> CacheGetResultImpl<T> getHit(final T value) {
		return new CacheGetResultImpl<T>(true, value);
	}

	/**
	 * Returns the singleton instance representing a cache miss.
	 *
	 * @return The singleton "miss" instance. Never <code>null</code>.
	 */
	@SuppressWarnings("unchecked")
	public static <T> CacheGetResultImpl<T> getMiss() {
		return (CacheGetResultImpl<T>) MISS;
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	/** Holds the actual value for this cache get result. Will be <code>null</code> for cache misses. */
	private final T value;
	/** Determines if this instance is a cache hit (<code>true</code>) or a miss (<code>false</code>). */
	private final boolean isHit;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	/**
	 * Creates a new instance of this class.
	 *
	 * <p>
	 * This constructor is <code>private</code> on purpose because it performs <b>no checks</b> on the arguments. Please
	 * make use of the static methods of this class instead.
	 *
	 * @param isHit
	 *            Use <code>true</code> if the new instance should represent a cache hit, or <code>false</code> if the
	 *            new instance should represent a cache miss.
	 *
	 * @param value
	 *            The actual value to be contained in the result. Will be used as the result for {@link #getValue()} in
	 *            the new instance.
	 *
	 */
	private CacheGetResultImpl(final boolean isHit, final T value) {
		this.isHit = isHit;
		this.value = value;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public boolean isHit() {
		return this.isHit;
	}

	@Override
	public T getValue() throws CacheGetResultNotPresentException {
		if (this.isHit) {
			return this.value;
		} else {
			throw new CacheGetResultNotPresentException("Do not call #getValue() when #isHit() is FALSE!");
		}
	}

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.isHit ? 1231 : 1237);
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
		CacheGetResultImpl<?> other = (CacheGetResultImpl<?>) obj;
		if (this.isHit != other.isHit) {
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

	// =====================================================================================================================
	// TO STRING
	// =====================================================================================================================

	@Override
	public String toString() {
		if (this.isHit) {
			return "CacheGetResult{HIT, value=" + this.getValue() + "}";
		} else {
			return "CacheGetResult{MISS}";
		}
	}
}
