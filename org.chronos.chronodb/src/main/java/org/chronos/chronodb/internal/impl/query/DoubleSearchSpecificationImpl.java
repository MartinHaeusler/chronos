package org.chronos.chronodb.internal.impl.query;

import java.util.function.Predicate;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

public class DoubleSearchSpecificationImpl extends AbstractSearchSpecification<Double, NumberCondition> implements DoubleSearchSpecification {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final double equalityTolerance;

	// =================================================================================================================
	// CONSTRUCTORS
	// =================================================================================================================

	public DoubleSearchSpecificationImpl(final String property, final NumberCondition condition, final double searchValue, final double equalityTolerance) {
		super(property, condition, searchValue);
		this.equalityTolerance = equalityTolerance;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public double getEqualityTolerance() {
		return this.equalityTolerance;
	}

	@Override
	public Predicate<Object> toFilterPredicate() {
		return (obj) -> {
			if (obj instanceof Double == false) {
				return false;
			}
			double value = (double) obj;
			return this.condition.applies(value, this.searchValue);
		};
	}

	@Override
	public SearchSpecification<Double> negate() {
		return new DoubleSearchSpecificationImpl(this.getProperty(), this.getCondition().negate(), this.getSearchValue(), this.getEqualityTolerance());
	}

}
