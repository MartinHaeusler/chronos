package org.chronos.chronodb.internal.impl.query.condition;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

public class EqualsCondition extends AbstractCondition implements StringCondition, NumberCondition {

	public static final EqualsCondition INSTANCE = new EqualsCondition();

	protected EqualsCondition() {
		super("==");
	}

	@Override
	public NotEqualsCondition negate() {
		return NotEqualsCondition.INSTANCE;
	}

	@Override
	public boolean applies(final String text, final String searchValue, final TextMatchMode matchMode) {
		switch (matchMode) {
		case CASE_INSENSITIVE:
			return text.toLowerCase().equals(searchValue.toLowerCase());
		case STRICT:
			return text.equals(searchValue);
		default:
			throw new UnknownEnumLiteralException(matchMode);
		}
	}

	@Override
	public boolean applies(final long value, final long searchValue) {
		return value == searchValue;
	}

	@Override
	public boolean applies(final double value, final double searchValue, final double equalityTolerance) {
		checkArgument(equalityTolerance >= 0, "Precondition violation: argument 'equalityTolerance' must not be negative!");
		double low = value - equalityTolerance;
		double high = value + equalityTolerance;
		if (low <= searchValue && searchValue <= high) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "Equals";
	}
}
