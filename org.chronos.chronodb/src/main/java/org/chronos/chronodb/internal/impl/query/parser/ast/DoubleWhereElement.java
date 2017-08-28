package org.chronos.chronodb.internal.impl.query.parser.ast;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

public class DoubleWhereElement extends WhereElement<Double, NumberCondition> {

	private final double equalityTolerance;

	public DoubleWhereElement(final String indexName, final NumberCondition condition, final double comparisonValue, final double equalityTolerance) {
		super(indexName, condition, comparisonValue);
		checkArgument(equalityTolerance >= 0, "Precondition violation - argument 'equalityTolerance' must not be negative!");
		this.equalityTolerance = equalityTolerance;
	}

	public double getEqualityTolerance() {
		return this.equalityTolerance;
	}

	@Override
	public DoubleWhereElement negate() {
		return new DoubleWhereElement(this.getIndexName(), this.getCondition().negate(), this.getComparisonValue(), this.getEqualityTolerance());
	}

	@Override
	public SearchSpecification<?> toSearchSpecification() {
		return DoubleSearchSpecification.create(this.getIndexName(), this.getCondition(), this.getComparisonValue(), this.getEqualityTolerance());
	}

}
