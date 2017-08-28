package org.chronos.chronodb.internal.impl.query.condition.string;

import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.chronodb.internal.impl.query.condition.NegatedStringCondition;

public class NotEndsWithCondition extends AbstractCondition implements NegatedStringCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final NotEndsWithCondition INSTANCE = new NotEndsWithCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected NotEndsWithCondition() {
		super("!-|");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public boolean acceptsEmptyValue() {
		return true;
	}

	@Override
	public EndsWithCondition negate() {
		return EndsWithCondition.INSTANCE;
	}

	@Override
	public String toString() {
		return "NotEndsWith";
	}

}
