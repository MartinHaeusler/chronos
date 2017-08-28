package org.chronos.chronodb.internal.impl.query;

import static com.google.common.base.Preconditions.*;

import java.util.function.Predicate;

import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;

public class StringSearchSpecificationImpl extends AbstractSearchSpecification<String, StringCondition> implements StringSearchSpecification {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	protected final TextMatchMode matchMode;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public StringSearchSpecificationImpl(final String property, final StringCondition condition, final String searchValue, final TextMatchMode matchMode) {
		super(property, condition, searchValue);
		checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
		this.matchMode = matchMode;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public TextMatchMode getMatchMode() {
		return this.matchMode;
	}

	@Override
	public Predicate<Object> toFilterPredicate() {
		return (obj) -> {
			if (obj instanceof String == false) {
				return false;
			}
			String value = (String) obj;
			return this.condition.applies(value, this.searchValue, this.matchMode);
		};
	}

	@Override
	public SearchSpecification<String> negate() {
		return new StringSearchSpecificationImpl(this.getProperty(), this.getCondition().negate(), this.getSearchValue(), this.getMatchMode());
	}

	// =================================================================================================================
	// HASH CODE & EQUALS
	// =================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.matchMode == null ? 0 : this.matchMode.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		StringSearchSpecificationImpl other = (StringSearchSpecificationImpl) obj;
		if (this.condition == null) {
			if (other.condition != null) {
				return false;
			}
		} else if (!this.condition.equals(other.condition)) {
			return false;
		}
		if (this.property == null) {
			if (other.property != null) {
				return false;
			}
		} else if (!this.property.equals(other.property)) {
			return false;
		}
		if (this.searchValue == null) {
			if (other.searchValue != null) {
				return false;
			}
		} else if (!this.searchValue.equals(other.searchValue)) {
			return false;
		}
		if (this.matchMode != other.matchMode) {
			return false;
		}
		return true;
	}
}
