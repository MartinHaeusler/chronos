package org.chronos.chronodb.api.query;

import java.util.List;

import org.chronos.chronodb.internal.impl.query.condition.number.GreaterThanCondition;
import org.chronos.chronodb.internal.impl.query.condition.number.GreaterThanOrEqualToCondition;
import org.chronos.chronodb.internal.impl.query.condition.number.LessThanCondition;
import org.chronos.chronodb.internal.impl.query.condition.number.LessThanOrEqualToCondition;

/**
 * A {@link NumberCondition} is a {@link Condition} on numeric (i.e. <code>int</code>, <code>long</code>, <code>float</code>, <code>double</code>...) values.
 *
 * <p>
 * This class comes with two different {@linkplain #applies(long, long) apply} methods, one for <code>long</code>s and one for <code>double</code>s.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface NumberCondition extends Condition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	/** The (strictly) greater-than condition <code>&gt;</code>. */
	public static final NumberCondition GREATER_THAN = GreaterThanCondition.INSTANCE;

	/** The greater-than-or-equal-to condition <code>&gt;=</code>. */
	public static final NumberCondition GREATER_EQUAL = GreaterThanOrEqualToCondition.INSTANCE;

	/** The (strictly) less-than condition <code>&lt;</code>. */
	public static final NumberCondition LESS_THAN = LessThanCondition.INSTANCE;

	/** The less-than-or-equal-to condition <code>&lt;=</code>. */
	public static final NumberCondition LESS_EQUAL = LessThanOrEqualToCondition.INSTANCE;

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Returns a list of all known {@link NumberCondition}s.
	 *
	 * @return The list of number conditions. Never <code>null</code>.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<NumberCondition> values() {
		List conditions = Condition.values();
		List<NumberCondition> resultList = conditions;
		resultList.add(GREATER_THAN);
		resultList.add(GREATER_EQUAL);
		resultList.add(LESS_THAN);
		resultList.add(LESS_EQUAL);
		return resultList;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public NumberCondition negate();

	/**
	 * Applies <code>this</code> condition to the given index and search value.
	 *
	 * @param value
	 *            The value from the secondary index. Must not be <code>null</code>.
	 * @param searchValue
	 *            The number to search for. Must not be <code>null</code>.
	 * @return <code>true</code> if this condition applies (matches) given the parameters, otherwise <code>false</code> .
	 */
	public boolean applies(final long value, final long searchValue);

	/**
	 * Applies <code>this</code> condition to the given index and search value.
	 *
	 * @param value
	 *            The value from the secondary index. Must not be <code>null</code>.
	 * @param searchValue
	 *            The number to search for. Must not be <code>null</code>.
	 * @return <code>true</code> if this condition applies (matches) given the parameters, otherwise <code>false</code> .
	 */
	public default boolean applies(final double value, final double searchValue) {
		return this.applies(value, searchValue, 0);
	}

	/**
	 * Applies <code>this</code> condition to the given index and search value.
	 *
	 * @param value
	 *            The value from the secondary index. Must not be <code>null</code>.
	 * @param searchValue
	 *            The number to search for. Must not be <code>null</code>.
	 * @param equalityTolerance
	 *            The tolerance range for equality conditions. Will be applied in positive AND negative direction. Must not be negative.
	 * @return <code>true</code> if this condition applies (matches) given the parameters, otherwise <code>false</code> .
	 */
	public boolean applies(final double value, final double searchValue, final double equalityTolerance);

}
