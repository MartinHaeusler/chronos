package org.chronos.chronodb.internal.impl.query.condition.string;

import java.util.regex.Pattern;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class MatchesRegexCondition extends AbstractCondition implements StringCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final MatchesRegexCondition INSTANCE = new MatchesRegexCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected MatchesRegexCondition() {
		super("matches");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public StringCondition negate() {
		return NotMatchesRegexCondition.INSTANCE;
	}

	@Override
	public boolean applies(final String text, final String searchValue, final TextMatchMode matchMode) {
		switch (matchMode) {
		case CASE_INSENSITIVE:
			return Pattern.matches("(?i)" + searchValue, text);
		case STRICT:
			return Pattern.matches(searchValue, text);
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
	}

	@Override
	public String toString() {
		return "MatchesRegex";
	}

}