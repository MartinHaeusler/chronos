package org.chronos.chronodb.internal.impl.query.condition.string;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.chronodb.internal.impl.query.condition.NegatedStringCondition;

public class NotStartsWithCondition extends AbstractCondition implements NegatedStringCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final NotStartsWithCondition INSTANCE = new NotStartsWithCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected NotStartsWithCondition() {
		super("!|-");
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
		return StartsWithCondition.INSTANCE;
	}

	@Override
	public String toString() {
		return "NotStartsWith";
	}
}
