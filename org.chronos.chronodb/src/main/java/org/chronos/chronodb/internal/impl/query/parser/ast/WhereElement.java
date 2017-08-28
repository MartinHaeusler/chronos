package org.chronos.chronodb.internal.impl.query.parser.ast;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

public abstract class WhereElement<VALUETYPE, CONDITIONTYPE extends Condition> implements QueryElement {

	// =================================================================================================================
	// PROPERTIES
	// =================================================================================================================

	protected final CONDITIONTYPE condition;
	protected final String indexName;
	protected final VALUETYPE comparisonValue;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public WhereElement(final String indexName, final CONDITIONTYPE condition, final VALUETYPE comparisonValue) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkNotNull(comparisonValue, "Precondition violation - argument 'comparisonValue' must not be NULL!");
		this.indexName = indexName;
		this.condition = condition;
		this.comparisonValue = comparisonValue;
	}

	// =================================================================================================================
	// public API
	// =================================================================================================================

	public CONDITIONTYPE getCondition() {
		return this.condition;
	}

	public String getIndexName() {
		return this.indexName;
	}

	public VALUETYPE getComparisonValue() {
		return this.comparisonValue;
	}

	public abstract WhereElement<VALUETYPE, CONDITIONTYPE> negate();

	public abstract SearchSpecification<?> toSearchSpecification();

	// =================================================================================================================
	// TO STRING
	// =================================================================================================================

	@Override
	public String toString() {
		return "where '" + this.indexName + "' " + this.condition.getInfix() + " '" + this.comparisonValue + "'";
	}
}
