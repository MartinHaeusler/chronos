package org.chronos.chronodb.internal.api.query.searchspec;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.impl.query.DoubleSearchSpecificationImpl;

public interface DoubleSearchSpecification extends SearchSpecification<Double> {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	public static DoubleSearchSpecification create(final String property, final NumberCondition condition, final double searchValue, final double equalityTolerance) {
		return new DoubleSearchSpecificationImpl(property, condition, searchValue, equalityTolerance);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public NumberCondition getCondition();

	public double getEqualityTolerance();

	@Override
	public default boolean matches(final Double value) {
		return this.getCondition().applies(value, this.getSearchValue(), this.getEqualityTolerance());
	}

	@Override
	public default String getDescriptiveSearchType() {
		return "Double";
	}
}
