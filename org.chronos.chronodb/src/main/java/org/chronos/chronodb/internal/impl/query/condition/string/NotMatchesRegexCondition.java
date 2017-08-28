package org.chronos.chronodb.internal.impl.query.condition.string;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.chronodb.internal.impl.query.condition.NegatedStringCondition;

public class NotMatchesRegexCondition extends AbstractCondition implements NegatedStringCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final NotMatchesRegexCondition INSTANCE = new NotMatchesRegexCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected NotMatchesRegexCondition() {
		super("!matches");
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
		return MatchesRegexCondition.INSTANCE;
	}

	@Override
	public String toString() {
		return "NotMatchesRegex";
	}

}