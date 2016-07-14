package org.chronos.chronodb.internal.api.query;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.impl.query.SearchSpecificationImpl;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

public interface SearchSpecification {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	public static SearchSpecification create(final String property, final Condition condition, final TextMatchMode matchMode, final String searchText) {
		return new SearchSpecificationImpl(property, condition, matchMode, searchText);
	}

	// =================================================================================================================
	// API
	// =================================================================================================================

	public String getProperty();

	public String getSearchText();

	public TextMatchMode getMatchMode();

	public Condition getCondition();

}