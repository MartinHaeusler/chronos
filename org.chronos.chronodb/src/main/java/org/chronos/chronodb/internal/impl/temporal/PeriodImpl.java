package org.chronos.chronodb.internal.impl.temporal;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.api.Period;

public final class PeriodImpl implements Period {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	private static final long FOREVER = Long.MAX_VALUE;

	private static final Period EMPTY = new PeriodImpl(0, 0);
	private static final Period ETERNAL = new PeriodImpl(0, FOREVER);

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	/**
	 * Creates a period that contains a range of timestamps.
	 *
	 * <p>
	 * This method can <b>not</b> be used to create empty periods. Please use {@link #empty()} instead.
	 *
	 * @param lowerBoundInclusive
	 *            The lower bound timestamp to be included in the new period. Must not be negative. Must be strictly
	 *            smaller than <code>upperBoundExclusive</code>.
	 * @param upperBoundExclusive
	 *            The upper bound timestamp to be exclused from the period. Must not be negative. Must be strictly
	 *            larger than <code>lowerBoundInclusive</code>.
	 *
	 * @return A new period with the given range of timestamps.
	 */
	public static Period createRange(final long lowerBoundInclusive, final long upperBoundExclusive) {
		checkArgument(lowerBoundInclusive >= 0, "Precondition violation - argument 'lowerBoundInclusive' must not be negative!");
		checkArgument(upperBoundExclusive >= 0, "Precondition violation - argument 'upperBoundExclusive' must not be negative!");
		checkArgument(lowerBoundInclusive < upperBoundExclusive, "Precondition violation - argument 'lowerBoundInclusive' must be strictly smaller than argument 'upperBoundExclusive'!");
		return new PeriodImpl(lowerBoundInclusive, upperBoundExclusive);
	}

	/**
	 * Creates a new {@link Period} that contains only one point in time, which is the given timestamp.
	 *
	 * <p>
	 * In other words, {@link #contains(long)} will always return <code>false</code> for all timestamps, except for the
	 * given one.
	 *
	 * <p>
	 * The lower bound of the period will be set to the given timestamp (as it is inclusive), the upper bound will be
	 * set to the given timestamp + 1 (exclusive).
	 *
	 * @param timestamp
	 *            The timestamp which should be contained in the resulting period. Must not be negative.
	 * @return A period that contains only the given timestamp. Never <code>null</code>.
	 */
	public static Period createPoint(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return new PeriodImpl(timestamp, timestamp + 1);
	}

	/**
	 * Creates a new {@link Period} that starts at the given lower bound (inclusive) and is open-ended.
	 *
	 * <p>
	 * In other words, {@link #contains(long)} will always return <code>true</code> for all timestamps greater than or
	 * equal to the given one.
	 *
	 * <p>
	 * Calling {@link #getUpperBound()} on the resulting period will return {@link Long#MAX_VALUE}.
	 *
	 * @param lowerBound
	 *            The lower bound to use for the new range period. Must not be negative.
	 *
	 * @return A period that starts at the given lower bound (inclusive) and is open-ended.
	 */
	public static Period createOpenEndedRange(final long lowerBound) {
		checkArgument(lowerBound >= 0, "Precondition violation - argument 'lowerBound' must not be negative!");
		return new PeriodImpl(lowerBound, FOREVER);
	}

	/**
	 * Returns an empty period.
	 *
	 * <p>
	 * An empty period is a period where {@link #contains(long)} always returns <code>false</code>, and the
	 * {@link #getLowerBound()} is greater than or equal to {@link #getUpperBound()}. In this implementation,
	 * {@link #getLowerBound()} and {@link #getUpperBound()} both return zero.
	 *
	 * @return The empty period. Never <code>null</code>.
	 */
	public static Period empty() {
		return EMPTY;
	}

	/**
	 * Returns the "eternal" period, i.e. the period that contains all non-negative timestamps.
	 *
	 * @return The eternal period. Never <code>null</code>.
	 */
	public static Period eternal() {
		return ETERNAL;
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	/** The lower bound of this period (inclusive). Always greater than or equal to zero. Immutable. */
	private final long lowerBound;
	/** The upper bound of this period (exclusive). Always greater than or equal to zero. Immutable. */
	private final long upperBound;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	/**
	 * Creates a new period.
	 *
	 * <p>
	 * There are <b>no checks</b> on the arguments in this constructor. It is assumed that the static factory methods
	 * provided by this class are used, which perform the required checks.
	 *
	 * @param lowerBound
	 *            The lower bound to use.
	 * @param upperBound
	 *            The upper bound to use.
	 */
	private PeriodImpl(final long lowerBound, final long upperBound) {
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public long getLowerBound() {
		return this.lowerBound;
	}

	@Override
	public long getUpperBound() {
		return this.upperBound;
	}

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (this.lowerBound ^ this.lowerBound >>> 32);
		result = prime * result + (int) (this.upperBound ^ this.upperBound >>> 32);
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
		PeriodImpl other = (PeriodImpl) obj;
		if (this.lowerBound != other.lowerBound) {
			return false;
		}
		if (this.upperBound != other.upperBound) {
			return false;
		}
		return true;
	}

	// =====================================================================================================================
	// TOSTRING
	// =====================================================================================================================

	@Override
	public String toString() {
		String upperBound = String.valueOf(this.getUpperBound());
		if (this.isOpenEnded()) {
			upperBound = "MAX";
		}
		return "Period[" + this.getLowerBound() + ";" + upperBound + ")";
	}
}
