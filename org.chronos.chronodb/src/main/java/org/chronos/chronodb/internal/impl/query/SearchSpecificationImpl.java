package org.chronos.chronodb.internal.impl.query;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.SearchSpecification;

public class SearchSpecificationImpl implements SearchSpecification {

	private final String property;
	private final String searchText;
	private final TextMatchMode matchMode;
	private final Condition condition;

	public SearchSpecificationImpl(final String property, final Condition condition, final TextMatchMode matchMode, final String searchText) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
		checkNotNull(searchText, "Precondition violation - argument 'searchText' must not be NULL!");
		this.property = property;
		this.condition = condition;
		this.matchMode = matchMode;
		this.searchText = searchText;
	}

	@Override
	public String getProperty() {
		return this.property;
	}

	@Override
	public String getSearchText() {
		return this.searchText;
	}

	@Override
	public TextMatchMode getMatchMode() {
		return this.matchMode;
	}

	@Override
	public Condition getCondition() {
		return this.condition;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Search['");
		builder.append(this.property);
		builder.append("' ");
		builder.append(this.condition);
		builder.append(" (");
		builder.append(this.matchMode);
		builder.append(") '");
		builder.append(this.searchText);
		builder.append("']");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.condition == null ? 0 : this.condition.hashCode());
		result = prime * result + (this.matchMode == null ? 0 : this.matchMode.hashCode());
		result = prime * result + (this.property == null ? 0 : this.property.hashCode());
		result = prime * result + (this.searchText == null ? 0 : this.searchText.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		SearchSpecificationImpl other = (SearchSpecificationImpl) obj;
		if (this.condition != other.condition) {
			return false;
		}
		if (this.matchMode != other.matchMode) {
			return false;
		}
		if (this.property == null) {
			if (other.property != null) {
				return false;
			}
		} else if (!this.property.equals(other.property)) {
			return false;
		}
		if (this.searchText == null) {
			if (other.searchText != null) {
				return false;
			}
		} else if (!this.searchText.equals(other.searchText)) {
			return false;
		}
		return true;
	}

}
