package org.chronos.chronodb.internal.impl.query.condition.string;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class EndsWithCondition extends AbstractCondition implements StringCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final EndsWithCondition INSTANCE = new EndsWithCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected EndsWithCondition() {
		super("-|");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public StringCondition negate() {
		return NotEndsWithCondition.INSTANCE;
	}

	@Override
	public boolean applies(final String text, final String searchValue, final TextMatchMode matchMode) {
		switch (matchMode) {
		case CASE_INSENSITIVE:
			return text.toLowerCase().endsWith(searchValue.toLowerCase());
		case STRICT:
			return text.endsWith(searchValue);
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
	}

	@Override
	public String toString() {
		return "EndsWith";
	}

}
