package org.chronos.chronodb.internal.impl.query.condition;

public class NotEqualsCondition extends AbstractCondition implements NegatedStringCondition, NegatedNumberCondition {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	public static final NotEqualsCondition INSTANCE = new NotEqualsCondition();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected NotEqualsCondition() {
		super("<>");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public boolean acceptsEmptyValue() {
		return true;
	}

	@Override
	public EqualsCondition negate() {
		return EqualsCondition.INSTANCE;
	}

	@Override
	public String toString() {
		return "NotEquals";
	}

}
