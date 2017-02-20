package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.impl.temporal.GetResultImpl;

/**
 * A {@link GetResult} is returned by a {@link TemporalDataMatrix#get(long, String)}.
 *
 * <p>
 * It represents the result of a {@link TemporalDataMatrix#get(long, String)} call and includes the actual
 * {@linkplain #getValue() value} as well as the associated {@linkplain #getPeriod() validity period}.
 *
 * <p>
 * Please note that the time range will <b>always include</b> the timestamp which was requested. Requesting different
 * timestamps may produce different ranges on the same key. It is guaranteed that these ranges <b>do not overlap</b> for
 * the same key.
 *
 * @param <T>
 *            The return type of the {@link #getValue()} method.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface GetResult<T> {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	/**
	 * Creates a new {@link GetResult} with no value and the given range.
	 *
	 * <p>
	 * The returned object will have {@link #isHit()} set to <code>false</code>.
	 *
	 * @param requestedKey
	 *            The key which was requested when this result is being produced. Must not be <code>null</code>.
	 * @param range
	 *            The range in which the result is valid. Must not be <code>null</code>.
	 *
	 *
	 * @return The new ranged result. Never <code>null</code>.
	 */
	public static <T> GetResult<T> createNoValueResult(final QualifiedKey requestedKey, final Period range) {
		return GetResultImpl.createNoValueResult(requestedKey, range);
	}

	/**
	 * Creates a new {@link GetResult} with the given value and range.
	 *
	 * @param requestedKey
	 *            The key which was requested when this result is being produced. Must not be <code>null</code>.
	 * @param value
	 *            The value which is to be contained in the result. May be <code>null</code> to indicate that there is
	 *            no value.
	 * @param range
	 *            The range in which the result is valid. Must not be <code>null</code>.
	 *
	 * @return The new ranged result. Never <code>null</code>.
	 */
	public static <T> GetResult<T> create(final QualifiedKey requestedKey, final T value, final Period range) {
		return GetResultImpl.create(requestedKey, value, range);
	}

	/**
	 * Creates a new {@link GetResult} that has the same properties as the given one, except it uses the given period
	 * instead of the original one.
	 *
	 * @param getResult
	 *            The original {@link GetResult} to take the properties from. Must not be <code>null</code>. Will not be
	 *            modified.
	 * @param newPeriod
	 *            The new period to use instead of the one contained in the given {@link GetResult}.
	 * @return A new {@link GetResult} that is exactly the same as the given one, except that it uses the given period.
	 */
	public static <T> GetResult<T> alterPeriod(final GetResult<T> getResult, final Period newPeriod) {
		return GetResultImpl.alterPeriod(getResult, newPeriod);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	/**
	 * Returns the {@link QualifiedKey} which was requested when this result instance was created.
	 *
	 * @return The requested qualified key. Never <code>null</code>.
	 */
	public QualifiedKey getRequestedKey();

	/**
	 * Returns the range of this result.
	 *
	 * <p>
	 * The range is a period that represents the set of timestamps in which the result is valid.
	 *
	 * @return The range period. Never <code>null</code>.
	 */
	public Period getPeriod();

	/**
	 * The value of the result.
	 *
	 * @return The value of this result. May be <code>null</code> to indicate that no value was found.
	 */
	public T getValue();

	/**
	 * Decides if this get result represents a "hit", i.e. the search ended up in a concrete entry.
	 *
	 * <p>
	 * This method returns <code>false</code> in case that no entry matched the query. It returns <code>true</code> if
	 * an entry matched the query, even if that entry represents a deletion.
	 *
	 * <p>
	 * If this method returns <code>true</code>, then {@link #getValue()} will always return <code>null</code> (no match
	 * found). Otherwise, {@link #getValue()} may return <code>null</code> to indicate an explicit deletion, or a non-
	 * <code>null</code> value to indicate an insert/update.
	 *
	 * @return <code>true</code> if the search produced a concrete result, or <code>false</code> if no key matched the
	 *         query.
	 */
	public boolean isHit();

}
