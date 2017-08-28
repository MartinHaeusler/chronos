package org.chronos.chronodb.internal.impl.query.condition.string;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.chronodb.internal.impl.query.condition.NegatedStringCondition;

public class NotContainsCondition extends AbstractCondition implements NegatedStringCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final NotContainsCondition INSTANCE = new NotContainsCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected NotContainsCondition() {
		super("!contains");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public boolean acceptsEmptyValue() {
		return true;
	}

	@Override
	public StringCondition negate() {
		return ContainsCondition.INSTANCE;
	}

	@Override
	public String toString() {
		return "NotContains";
	}

}
