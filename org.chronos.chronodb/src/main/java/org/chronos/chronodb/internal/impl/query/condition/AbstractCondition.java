package org.chronos.chronodb.internal.impl.query.condition;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.Condition;

public abstract class AbstractCondition implements Condition {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	/** The in-fix representation for this literal. */
	private String infix;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	/**
	 * Constructs a new condition, for internal use only.
	 *
	 * @param infixRepresentation
	 *            The in-fix representation of the condition. Must not be <code>null</code>.
	 */
	protected AbstractCondition(final String infixRepresentation) {
		checkNotNull(infixRepresentation, "Precondition violation - argument 'infixRepresentation' must not be NULL!");
		this.infix = infixRepresentation;
	}

	@Override
	public String getInfix() {
		return this.infix;
	}

}
