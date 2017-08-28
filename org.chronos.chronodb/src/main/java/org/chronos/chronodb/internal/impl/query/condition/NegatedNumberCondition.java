package org.chronos.chronodb.internal.impl.query.condition;

import org.chronos.chronodb.api.query.NumberCondition;

public interface NegatedNumberCondition extends NumberCondition, NegatedCondition {

	@Override
	default boolean applies(final long value, final long searchValue) {
		return this.negate().applies(value, searchValue) == false;
	}

	@Override
	public default boolean applies(final double value, final double searchValue, final double equalityTolerance) {
		return this.negate().applies(value, searchValue, equalityTolerance) == false;
	}

}
