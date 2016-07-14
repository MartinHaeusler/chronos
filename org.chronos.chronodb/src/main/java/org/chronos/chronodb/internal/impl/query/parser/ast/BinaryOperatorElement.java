package org.chronos.chronodb.internal.impl.query.parser.ast;

import static com.google.common.base.Preconditions.*;

public class BinaryOperatorElement implements QueryElement {

	// =================================================================================================================
	// PROPERTIES
	// =================================================================================================================

	private final BinaryQueryOperator operator;

	private final QueryElement leftChild;
	private final QueryElement rightChild;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public BinaryOperatorElement(final QueryElement left, final BinaryQueryOperator operator,
			final QueryElement right) {
		checkNotNull(left, "Precondition violation - argument 'left' must not be NULL!");
		checkNotNull(operator, "Precondition violation - argument 'operator' must not be NULL!");
		checkNotNull(right, "Precondition violation - argument 'right' must not be NULL!");
		this.leftChild = left;
		this.rightChild = right;
		this.operator = operator;
	}

	// =================================================================================================================
	// GETTERS & SETTERS
	// =================================================================================================================

	public BinaryQueryOperator getOperator() {
		return this.operator;
	}

	public QueryElement getLeftChild() {
		return this.leftChild;
	}

	public QueryElement getRightChild() {
		return this.rightChild;
	}

	// =================================================================================================================
	// TO STRING
	// =================================================================================================================

	@Override
	public String toString() {
		return "(" + this.leftChild + ") " + this.operator + " (" + this.rightChild + ")";
	}
}
