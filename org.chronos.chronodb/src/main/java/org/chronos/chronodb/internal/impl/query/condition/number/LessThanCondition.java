package org.chronos.chronodb.internal.impl.query.condition.number;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.chronodb.internal.impl.query.condition.NegatedNumberCondition;

public class LessThanCondition extends AbstractCondition implements NegatedNumberCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final LessThanCondition INSTANCE = new LessThanCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected LessThanCondition() {
		super("<");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public NumberCondition negate() {
		return GreaterThanOrEqualToCondition.INSTANCE;
	}

	@Override
	public String toString() {
		return "LessThan";
	}

}