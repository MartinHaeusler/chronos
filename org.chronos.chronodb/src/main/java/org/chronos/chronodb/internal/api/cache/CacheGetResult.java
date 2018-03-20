package org.chronos.chronodb.internal.api.cache;

import org.chronos.chronodb.api.exceptions.CacheGetResultNotPresentException;
import org.chronos.chronodb.internal.impl.cache.CacheGetResultImpl;

/**
 * A {@link CacheGetResult} represents the result of calling
 * {@link ChronoDBCache#get(String, long, org.chronos.chronodb.api.key.QualifiedKey)}.
 *
 * <p>
 * An instance of this class may either represent a cache <i>hit</i>, in which case {@link #isHit()} returns
 * <code>true</code> and {@link #getValue()} delivers the actual result data, or it may represent a cache <i>miss</i> ,
 * in which case {@link #isHit()} returns <code>false</code> and {@link #getValue()} will always throw a
 * {@link CacheGetResultNotPresentException}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <T>
 *            The type of the actual result data contained in this cache get result. Used for the return type of
 *            {@link #getValue()}.
 */
public interface CacheGetResult<T> {

	/**
	 * Returns the singleton {@link CacheGetResult} instance representing a cache miss.
	 *
	 * <p>
	 * Calling {@link #isHit()} on the returned instance will always return <code>false</code>. Calling
	 * {@link #getValue()} on the returned instance will always throw a {@link CacheGetResultNotPresentException}.
	 *
	 * @return The singleton instance representing a cache miss. Never <code>null</code>.
	 *
	 * @param <T>
	 *            The data type of the value returned by {@link #getValue()}. In this case, this parameter is never
	 *            used, as {@link #getValue()} always throws a {@link CacheGetResultNotPresentException}.
	 */
	public static <T> CacheGetResult<T> miss() {
		return CacheGetResultImpl.getMiss();
	}

	/**
	 * Creates a new {@link CacheGetResult} instance representing a cache hit.
	 *
	 * <p>
	 * An instance created by this method will never throw a {@link CacheGetResultNotPresentException} on
	 * {@link #getValue()}.
	 *
	 * @param value
	 *            The result value to return in {@link #getValue()} in the new instance.
	 * @param validFrom
	 *            The insertion timestamp of the given value into the store, i.e. the greatest change timestamp of the
	 *            request key that is less than or equal to the transaction timestamp. Must not be negative.
	 *
	 * @return The newly created cache get result instance. Never <code>null</code>.
	 *
	 * @param <T>
	 *            The data type of the value returned by {@link #getValue()}.
	 */
	public static <T> CacheGetResult<T> hit(final T value, final long validFrom) {
		return CacheGetResultImpl.getHit(value, validFrom);
	}

	/**
	 * Checks if this cache result represents a "cache hit".
	 *
	 * <p>
	 * If this method returns <code>true</code>, then {@link #getValue()} returns the actual result value. If this
	 * method returns <code>false</code>, then {@link #getValue()} will always throw a
	 * {@link CacheGetResultNotPresentException}.
	 *
	 * @return <code>true</code> if this result represents a cache hit, or <code>false</code> if it represents a cache
	 *         miss.
	 *
	 * @see #isMiss()
	 */
	public boolean isHit();

	/**
	 * Returns the value associated with this cache query result.
	 *
	 * <p>
	 * If {@link #isHit()} returns <code>true</code>, then this method will return the actual result value. If
	 * {@link #isHit()} returns <code>false</code>, this method will always throw a
	 * {@link CacheGetResultNotPresentException}.
	 *
	 * @return The actual query result.
	 *
	 * @throws CacheGetResultNotPresentException
	 *             Thrown if this instance represents a cache miss (<code>this.</code>{@link #isHit()} returns
	 *             <code>false</code>) and <code>this.</code>{@link #getValue()} is called.
	 */
	public T getValue() throws CacheGetResultNotPresentException;

	/**
	 * Returns the timestamp from which onward the {@link #getValue() value} was valid in the store.
	 *
	 * <p>
	 * Please note that the returned value will always be less than or equal to the timestamp of your current
	 * transaction; newer values may have been set on this key after the transaction timestamp. Those newer timestamps
	 * will not be reflected here. In other words, this method returns the greatest change timestamp of the request key
	 * which is less than or equal to the transaction timestamp.
	 *
	 * @return The "valid from" timestamp of the key-value pair.
	 * @throws CacheGetResultNotPresentException
	 *             Thrown if this instance represents a cache miss (<code>this.</code>{@link #isHit()} returns
	 *             <code>false</code>) and <code>this.</code>{@link #getValidFrom()} is called.
	 */
	public long getValidFrom() throws CacheGetResultNotPresentException;

	/**
	 * Checks if this cache result represents a "cache miss".
	 *
	 * <p>
	 * If this method returns <code>true</code>, then {@link #getValue()} will always throw a
	 * {@link CacheGetResultNotPresentException}. If this method returns <code>false</code>, then {@link #getValue()}
	 * will return the actual result data.
	 *
	 * @return <code>true</code> if this result represents a cache miss, or <code>false</code> if it represents a cache
	 *         hit.
	 *
	 * @see #isHit()
	 */
	public default boolean isMiss() {
		return this.isHit() == false;
	}

}
