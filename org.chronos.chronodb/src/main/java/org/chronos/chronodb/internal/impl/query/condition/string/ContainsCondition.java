package org.chronos.chronodb.internal.impl.query.condition.string;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class ContainsCondition extends AbstractCondition implements StringCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final ContainsCondition INSTANCE = new ContainsCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected ContainsCondition() {
		super("contains");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public StringCondition negate() {
		return NotContainsCondition.INSTANCE;
	}

	@Override
	public boolean applies(final String text, final String searchValue, final TextMatchMode matchMode) {
		switch (matchMode) {
		case CASE_INSENSITIVE:
			return text.toLowerCase().contains(searchValue.toLowerCase());
		case STRICT:
			return text.contains(searchValue);
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
	}

	@Override
	public String toString() {
		return "Contains";
	}

}
