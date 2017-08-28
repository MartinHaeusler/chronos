package org.chronos.chronodb.internal.impl.query.condition;

import org.chronos.chronodb.api.query.Condition;

public interface NegatedCondition extends Condition {

	@Override
	default boolean isNegated() {
		return true;
	}

}
