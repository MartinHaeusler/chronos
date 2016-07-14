package org.chronos.chronodb.internal.impl.query.parser.ast;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

public class WhereElement implements QueryElement {

	// =================================================================================================================
	// PROPERTIES
	// =================================================================================================================

	private final Condition condition;
	private final TextMatchMode matchMode;
	private final String indexName;
	private final String comparisonValue;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public WhereElement(final String indexName, final Condition condition, final TextMatchMode matchMode,
			final String comparisonValue) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
		checkNotNull(comparisonValue, "Precondition violation - argument 'comparisonValue' must not be NULL!");
		this.indexName = indexName;
		this.condition = condition;
		this.matchMode = matchMode;
		this.comparisonValue = comparisonValue;
	}

	// =================================================================================================================
	// GETTERS & SETTERS
	// =================================================================================================================

	public Condition getCondition() {
		return this.condition;
	}

	public String getIndexName() {
		return this.indexName;
	}

	public String getComparisonValue() {
		return this.comparisonValue;
	}

	public TextMatchMode getMatchMode() {
		return this.matchMode;
	}

	// =================================================================================================================
	// TO STRING
	// =================================================================================================================

	@Override
	public String toString() {
		return "where '" + this.indexName + "' " + this.condition.getInfix() + " '" + this.comparisonValue + "'";
	}
}
