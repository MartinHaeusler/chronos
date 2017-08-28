package org.chronos.chronodb.internal.api.query.searchspec;

import java.util.function.Predicate;

import org.chronos.chronodb.api.query.Condition;

public interface SearchSpecification<T> {

	// =================================================================================================================
	// API
	// =================================================================================================================

	public String getProperty();

	public T getSearchValue();

	public Condition getCondition();

	public Predicate<Object> toFilterPredicate();

	public SearchSpecification<T> negate();

	public boolean matches(final T value);

	public String getDescriptiveSearchType();

}