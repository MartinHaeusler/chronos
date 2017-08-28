package org.chronos.chronodb.api.query;

import java.util.List;

import org.chronos.chronodb.internal.impl.query.condition.EqualsCondition;
import org.chronos.chronodb.internal.impl.query.condition.NotEqualsCondition;
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement;

import com.google.common.collect.Lists;

/**
 * A condition is a comparison operation to be applied inside a {@link WhereElement} in the query language.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface Condition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	/** Standard equality condition. */
	public static final EqualsCondition EQUALS = EqualsCondition.INSTANCE;

	/** Inverted equality condition. */
	public static final NotEqualsCondition NOT_EQUALS = NotEqualsCondition.INSTANCE;

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	/**
	 * Returns the list of all known {@link Condition}s (excluding instances of subclasses).
	 *
	 * @return The list of known conditions. Never <code>null</code>.
	 */
	public static List<Condition> values() {
		return Lists.newArrayList(EQUALS, NOT_EQUALS);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Returns the negated form of this condition.
	 *
	 * <p>
	 * This also works for already negated conditions. The following statement is always true:
	 *
	 * <pre>
	 * condition.getNegated().getNegated() == condition // always true
	 * </pre>
	 *
	 * @return The negated condition. For already negated conditions, the regular non-negated condition will be returned.
	 */
	public Condition negate();

	/**
	 * Checks if this condition is negated or not.
	 *
	 * @return <code>true</code> if this is a negated condition, otherwise <code>false</code>.
	 */
	public default boolean isNegated() {
		return false;
	}

	/**
	 * Checks if this condition accepts the empty value.
	 *
	 * @return <code>true</code> if this condition accepts the empty value, otherwise <code>false</code>.
	 */
	public default boolean acceptsEmptyValue() {
		return false;
	}

	/**
	 * Returns the in-fix representation of this condition.
	 *
	 * @return The in-fix representation. Never <code>null</code>.
	 */
	public String getInfix();

}
