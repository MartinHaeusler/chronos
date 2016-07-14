package org.chronos.chronodb.internal.api;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.impl.temporal.PeriodImpl;

/**
 * A {@link Period} is a range (or interval) of timestamps.
 *
 * <p>
 * This interface represents a generic, general purpose Period. The period is defined by two {@link Long} values:
 * <ul>
 * <li>The lower bound (see: {@link #getLowerBound()}). This value is always <b>included</b> in the period.
 * <li>The upper bound (see: {@link #getUpperBound()}). This value is always <b>excluded</b> from the period.
 * </ul>
 *
 * In mathematical notation, the period <code>P</code> is therefore defined as:
 *
 * <pre>
 *     P := [lower;upper[
 * </pre>
 *
 * Containment of a given timestamp in a given period can be determined via {@link #contains(long)}. Note that this
 * class also implements {@link Comparable}, by comparing the <b>lower bounds</b> of any two periods.
 *
 * <p>
 * A period can also be <i>empty</i>, in which case {@link #isEmpty()} returns <code>true</code>. In an empty period,
 * {@link #contains(long)} will always return <code>false</code>. The bounds of an empty period can be arbitrary, but
 * the lower bound must be at least as large as the upper bound, and both bounds must refer to non-negative values.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface Period extends Comparable<Period> {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

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
		return PeriodImpl.empty();
	}

	/**
	 * Returns the "eternal" period, i.e. the period that contains all non-negative timestamps.
	 *
	 * @return The eternal period. Never <code>null</code>.
	 */
	public static Period eternal() {
		return PeriodImpl.eternal();
	}

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
		return PeriodImpl.createRange(lowerBoundInclusive, upperBoundExclusive);
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
		return PeriodImpl.createOpenEndedRange(lowerBound);
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
		return PeriodImpl.createPoint(timestamp);
	}

	// =====================================================================================================================
	// API
	// =====================================================================================================================

	/**
	 * Returns the timestamp that represents the lower bound of this period.
	 *
	 * <p>
	 * The lower bound is always included in a period. Therefore calling:
	 *
	 * <pre>
	 * period.contains(period.getLowerBound())
	 * </pre>
	 *
	 * ... always returns <code>true</code>, <i>unless</i> the period is empty.
	 *
	 * <p>
	 * The lower bound timestamp is always a value greater than or equal to zero. In all non-empty periods, the lower
	 * bound is strictly smaller than the upper bound.
	 *
	 * @return The timestamp that represents the lower bound.
	 */
	public long getLowerBound();

	/**
	 * Returns the timestamp that represents the upper bound of this period.
	 *
	 * <p>
	 * The upper bound is always excluded from a period. Therefore calling:
	 *
	 * <pre>
	 * period.contains(period.getUpperBound())
	 * </pre>
	 *
	 * ... always returns <code>false</code>.
	 *
	 * <p>
	 * The upper bound is a always a value greater than or equal to zero. In all non-empty periods, the upper bound is
	 * strictly larger than the lower bound.
	 *
	 * @return The timestamp that represents the upper bound.
	 */
	public long getUpperBound();

	// =====================================================================================================================
	// DEFAULT IMPLEMENTATIONS
	// =====================================================================================================================

	/**
	 * Checks if the given timestamp is contained in (i.e. part of) this period.
	 *
	 * <p>
	 * A timestamp is contained in a period if the following condition holds:
	 *
	 * <pre>
	 * p.lowerBound <= timestamp < p.upperBound
	 * </pre>
	 *
	 * <p>
	 * This method always returns <code>false</code> (regardless of the argument) if this period is empty.
	 *
	 * @param timestamp
	 *            The timestamp to check. Must not be negative.
	 *
	 * @return <code>true</code> if the given timestamp is contained in this period, otherwise <code>false</code>.
	 */
	public default boolean contains(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		if (this.isEmpty()) {
			return false;
		}
		return this.getLowerBound() <= timestamp && timestamp < this.getUpperBound();
	}

	/**
	 * Checks if this period is empty or not.
	 *
	 * <p>
	 * A period <code>p</code> is empty if and only if there is no valid timestamp <code>t</code> for which
	 *
	 * <pre>
	 * p.contains(t)
	 * </pre>
	 *
	 * returns <code>true</code>. In empty periods, {@link #getLowerBound()} always returns a value that is greater than
	 * or equal to the value returned by {@link #getUpperBound()}.
	 *
	 * @return <code>true</code> if this period is empty, otherwise <code>false</code>.
	 */
	public default boolean isEmpty() {
		return this.getLowerBound() >= this.getUpperBound();
	}

	/**
	 * Checks if there is an overlap between this period and the given period.
	 *
	 * <p>
	 * Two periods <code>P1</code> and <code>P2</code> overlap each other, if there is at least one timestamp
	 * <code>t</code> where
	 *
	 * <pre>
	 * P1.contains(t) && P2.contains(t)
	 * </pre>
	 *
	 * ... returns <code>true</code>. In other words, two periods overlap if there is at least one timestamp which is
	 * contained in both periods.
	 *
	 * <p>
	 * If either <code>this</code> period or the <code>other</code> period is empty, this method will always return
	 * <code>false</code>.
	 *
	 * @param other
	 *            The period to check for an overlap with. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if there is an overlap between this and the other period, otherwise <code>false</code>.
	 */
	public default boolean overlaps(final Period other) {
		checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
		if (this.isEmpty() || other.isEmpty()) {
			// there is never an overlap between empty and other periods
			return false;
		}
		if (this.isStrictlyAfter(other)) {
			return false;
		}
		if (this.isStrictlyBefore(other)) {
			return false;
		}
		// if this period is neither strictly before, nor strictly after the other period, then there must be an overlap
		return true;
	}

	/**
	 * Checks if this period fully contains the other period.
	 *
	 * <p>
	 * A period <code>P1</code> contains a period <code>P2</code> if, for all timestamps <code>t</code> in
	 * <code>P2</code>
	 *
	 * <pre>
	 * P1.contains(t)
	 * </pre>
	 *
	 * ... returns <code>true</code>. In other words, a period is contained in another period if all timestamps from the
	 * first period are also contained in the second period.
	 *
	 * <p>
	 * If either <code>this</code> period or the <code>other</code> period is empty, this method returns
	 * <code>false</code>.
	 *
	 * @param other
	 *            The other period to check if it is contained in this period. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if this period contains the other period, otherwise <code>false</code>.
	 */
	public default boolean contains(final Period other) {
		checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
		if (this.isEmpty() || other.isEmpty()) {
			// there is never a containment relationship between empty and other periods
			return false;
		}
		if (this.getLowerBound() <= other.getLowerBound() && this.getUpperBound() >= other.getUpperBound()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if this period is adjacent to the other period.
	 *
	 * <p>
	 * A period <code>P1</code> is adjacent to another period <code>P2</code> if:
	 *
	 * <pre>
	 * P1.upperBound == P2.lowerBound || P1.lowerBound == P2.upperBound
	 * </pre>
	 *
	 * In other words, two periods are adjacent to each other if there is no timestamp that is between them, but not
	 * part of either one of them.
	 *
	 * <p>
	 * Note that overlapping periods are never adjacent to each other.
	 *
	 * <p>
	 * If either <code>this</code> period or the <code>other</code> period is empty, this method returns
	 * <code>false</code>.
	 *
	 * @param other
	 *            The other period to check adjacency against. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if this period is adjacent to the other period, otherwise <code>false</code>.
	 */
	public default boolean isAdjacentTo(final Period other) {
		checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
		if (this.isEmpty() || other.isEmpty()) {
			// there is never an adjacency relationship between empty and other periods
			return false;
		}
		if (this.getLowerBound() == other.getUpperBound() || this.getUpperBound() == other.getLowerBound()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if this period starts before the other period.
	 *
	 * <p>
	 * A period <code>P1</code> is before another period <code>P2</code> if and only if:
	 *
	 * <pre>
	 * P1.lowerBound < P2.lowerBound
	 * </pre>
	 *
	 * Note that there is no restriction on the upper bounds. Therefore, partially overlapping periods can also be in an
	 * "is before" relation with each other.
	 *
	 * <p>
	 * If either <code>this</code> period or the <code>other</code> period is empty, this method returns
	 * <code>false</code>.
	 *
	 * @param other
	 *            The other period to check the "is before" relation against. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if this period is before the other period, otherwise <code>false</code>.
	 *
	 * @see #isAfter(Period)
	 * @see #isStrictlyBefore(Period)
	 * @see #isStrictlyAfter(Period)
	 */
	public default boolean isBefore(final Period other) {
		checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
		if (this.isEmpty() || other.isEmpty()) {
			// there is never an "is before" relationship between empty and other periods
			return false;
		}
		if (this.getLowerBound() < other.getLowerBound()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if this period starts before the given timestamp (and does not include it).
	 *
	 * A period <code>P</code> is before a timestamp <code>T</code> if and only if:
	 *
	 * <pre>
	 * P.upperBound <= T
	 * </pre>
	 *
	 * <p>
	 * If this period is empty, this method returns <code>false</code>.
	 *
	 * @param timestamp
	 *            The timestamp to check the "is before" relation against. Must not be negative.
	 * @return <code>true</code> if this period is before the given timestamp, otherwise <code>false</code>.
	 */
	public default boolean isBefore(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		if (this.isEmpty()) {
			// by definition...
			return false;
		}
		if (this.getLowerBound() <= timestamp) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if this period starts after the other period.
	 *
	 * <p>
	 * A period <code>P1</code> is after another period <code>P2</code> if and only if:
	 *
	 * <pre>
	 * P1.lowerBound > P2.lowerBound
	 * </pre>
	 *
	 * Note that there is no restriction on the upper bounds. Therefore, partially overlapping periods can also be in an
	 * "is after" relation with each other.
	 *
	 * <p>
	 * If either <code>this</code> period or the <code>other</code> period is empty, this method returns
	 * <code>false</code>.
	 *
	 * @param other
	 *            The other period to check the "is after" relation against. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if this period is after the other period, otherwise <code>false</code>.
	 *
	 * @see #isBefore(Period)
	 * @see #isStrictlyBefore(Period)
	 * @see #isStrictlyAfter(Period)
	 */
	public default boolean isAfter(final Period other) {
		checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
		if (this.isEmpty() || other.isEmpty()) {
			// there is never an "is after" relationship between empty and other periods
			return false;
		}
		if (this.getLowerBound() > other.getLowerBound()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if this period starts after the given timestamp.
	 *
	 * A period <code>P</code> is after a timestamp <code>T</code> if and only if:
	 *
	 * <pre>
	 * P.lowerBound > T
	 * </pre>
	 *
	 * <p>
	 * If this period is empty, this method returns <code>false</code>.
	 *
	 * @param timestamp
	 *            The timestamp to check the "is after" relation against. Must not be negative.
	 * @return <code>true</code> if this period is after the given timestamp, otherwise <code>false</code>.
	 */
	public default boolean isAfter(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		if (this.isEmpty()) {
			// by definition...
			return false;
		}
		if (this.getLowerBound() > timestamp) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if this period starts and ends before the other period starts.
	 *
	 * <p>
	 * A period <code>P1</code> is strictly before another period <code>P2</code> if and only if:
	 *
	 * <pre>
	 * P1.lowerBound < P2.lowerBound && P1.upperBound <= P2.lowerBound
	 * </pre>
	 *
	 * <p>
	 * If either <code>this</code> period or the <code>other</code> period is empty, this method returns
	 * <code>false</code>.
	 *
	 * @param other
	 *            The other period to check the "is strictly before" relation against. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if this period is strictly before the other period, otherwise <code>false</code>.
	 *
	 * @see #isBefore(Period)
	 * @see #isAfter(Period)
	 * @see #isStrictlyAfter(Period)
	 */
	public default boolean isStrictlyBefore(final Period other) {
		checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
		if (this.isEmpty() || other.isEmpty()) {
			// there is never an "is strictly before" relationship between empty and other periods
			return false;
		}
		if (this.getLowerBound() < other.getLowerBound() && this.getUpperBound() <= other.getLowerBound()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if this period starts after the other period ends.
	 *
	 * <p>
	 * A period <code>P1</code> is strictly after another period <code>P2</code> if and only if:
	 *
	 * <pre>
	 * P1.lowerBound >= P2.upperBound
	 * </pre>
	 *
	 * <p>
	 * If either <code>this</code> period or the <code>other</code> period is empty, this method returns
	 * <code>false</code>.
	 *
	 * @param other
	 *            The other period to check the "is strictly after" relation against. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if this period is strictly after the other period, otherwise <code>false</code>.
	 *
	 * @see #isBefore(Period)
	 * @see #isAfter(Period)
	 * @see #isStrictlyBefore(Period)
	 */
	public default boolean isStrictlyAfter(final Period other) {
		checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
		if (this.isEmpty() || other.isEmpty()) {
			// there is never an "is strictly after" relationship between empty and other periods
			return false;
		}
		if (this.getLowerBound() >= other.getUpperBound()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks if this period has an open end.
	 *
	 * <p>
	 * A period is open-ended if {@link #getUpperBound()} is equal to {@link Long#MAX_VALUE}.
	 *
	 * <p>
	 * Note that an empty period is never open-ended. For empty periods, this method always returns <code>false</code>.
	 *
	 * @return <code>true</code> if this period is open-ended, otherwise <code>false</code>.
	 */
	public default boolean isOpenEnded() {
		if (this.isEmpty()) {
			// there can never be an open-ended empty period
			return false;
		}
		return this.getUpperBound() == Long.MAX_VALUE;
	}

	/**
	 * Returns the length of this period.
	 *
	 * <p>
	 * The length of a period is defined as the number of different timestamps which are contained in this period.
	 *
	 * <p>
	 * For the empty period, this method always returns zero. For all other periods, a non-negative number is returned.
	 *
	 * @return The number of different timestamps contained in this period.
	 */
	public default long length() {
		if (this.isEmpty()) {
			return 0;
		}
		return this.getUpperBound() - this.getLowerBound();
	}

	/**
	 * Creates a duplicate of this period, replacing the original upper bound with the given upper bound.
	 *
	 * <p>
	 * <code>this</code> will not be modified.
	 *
	 * <p>
	 * Please note that this method does not allow to create empty ranges, i.e. the new upper bound must always be
	 * strictly larger than the lower bound.
	 *
	 * <p>
	 * This operation is only allowed on non-empty periods. This method throws an {@link IllegalStateException} if the
	 * original period is empty.
	 *
	 * @param newUpperBound
	 *            The new upper bound to use. Must not be negative.
	 *
	 * @return The new period with the given upper bound.
	 */
	public default Period setUpperBound(final long newUpperBound) {
		checkArgument(newUpperBound > this.getLowerBound(),
				"Precondition violation - argument 'newUpperBound' must be strictly larger than the current lower bound!");
		if (this.isEmpty()) {
			throw new IllegalStateException("Cannot use #setUpperBound(...) on empty periods!");
		}
		return Period.createRange(getLowerBound(), newUpperBound);
	}

	/**
	 * Compares this period to the given period.
	 *
	 * <p>
	 * Two periods are compared by comparing the <b>lower bounds</b>. The upper bounds are <b>ignored</b> in the
	 * comparison.
	 *
	 * @param other
	 *            The other period to compare this period to
	 *
	 * @return A value less than, equal to, or larger than zero if <code>this</code> period is less than, equal to, or
	 *         larger than the <code>other</code> period.
	 */
	@Override
	public default int compareTo(final Period other) {
		if (other == null) {
			return 1;
		}
		if (other.isEmpty() && this.isEmpty() == false) {
			return 1;
		}
		return Long.compare(this.getLowerBound(), other.getLowerBound());
	}

}
