package org.chronos.chronodb.internal.impl.query.parser.ast;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

public class StringWhereElement extends WhereElement<String, StringCondition> {

	protected final TextMatchMode matchMode;

	public StringWhereElement(final String indexName, final StringCondition condition, final TextMatchMode matchMode, final String comparisonValue) {
		super(indexName, condition, comparisonValue);
		checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
		this.matchMode = matchMode;
	}

	public TextMatchMode getMatchMode() {
		return this.matchMode;
	}

	@Override
	public StringWhereElement negate() {
		return new StringWhereElement(this.getIndexName(), this.getCondition().negate(), this.getMatchMode(), this.getComparisonValue());
	}

	@Override
	public SearchSpecification<?> toSearchSpecification() {
		return StringSearchSpecification.create(this.getIndexName(), this.getCondition(), this.getMatchMode(), this.getComparisonValue());
	}

}
