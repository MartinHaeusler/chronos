package org.chronos.chronodb.internal.impl.query.condition.number;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.chronodb.internal.impl.query.condition.NegatedNumberCondition;

public class LessThanOrEqualToCondition extends AbstractCondition implements NegatedNumberCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final LessThanOrEqualToCondition INSTANCE = new LessThanOrEqualToCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected LessThanOrEqualToCondition() {
		super("<=");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public NumberCondition negate() {
		return GreaterThanCondition.INSTANCE;
	}

	@Override
	public String toString() {
		return "LessThanOrEqualTo";
	}

}