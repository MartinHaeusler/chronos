package org.chronos.chronodb.internal.impl.query.condition;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

public interface NegatedStringCondition extends StringCondition, NegatedCondition {

	@Override
	public StringCondition negate();

	@Override
	default boolean applies(final String text, final String searchValue, final TextMatchMode matchMode) {
		return this.negate().applies(text, searchValue, matchMode) == false;
	}
}
