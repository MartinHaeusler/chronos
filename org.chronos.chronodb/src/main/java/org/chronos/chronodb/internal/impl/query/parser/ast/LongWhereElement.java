package org.chronos.chronodb.internal.impl.query.parser.ast;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

public class LongWhereElement extends WhereElement<Long, NumberCondition> {

	public LongWhereElement(final String indexName, final NumberCondition condition, final Long comparisonValue) {
		super(indexName, condition, comparisonValue);
	}

	@Override
	public LongWhereElement negate() {
		return new LongWhereElement(this.getIndexName(), this.getCondition().negate(), this.getComparisonValue());
	}

	@Override
	public SearchSpecification<?> toSearchSpecification() {
		return LongSearchSpecification.create(this.getIndexName(), this.getCondition(), this.getComparisonValue());
	}
}
