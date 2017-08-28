package org.chronos.chronodb.internal.impl.query.condition.number;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;

public class GreaterThanOrEqualToCondition extends AbstractCondition implements NumberCondition {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final GreaterThanOrEqualToCondition INSTANCE = new GreaterThanOrEqualToCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected GreaterThanOrEqualToCondition() {
		super(">=");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public NumberCondition negate() {
		return LessThanCondition.INSTANCE;
	}

	@Override
	public boolean applies(final long value, final long searchValue) {
		return value >= searchValue;
	}

	@Override
	public boolean applies(final double value, final double searchValue, final double equalityTolerance) {
		checkArgument(equalityTolerance >= 0, "Precondition violation: argument 'equalityTolerance' must not be negative!");
		return value >= searchValue;
	}

	@Override
	public String toString() {
		return "GreaterThanOrEqualTo";
	}
}
