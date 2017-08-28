package org.chronos.chronodb.internal.impl.query;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

public abstract class AbstractSearchSpecification<VALUETYPE, CONDITIONTYPE extends Condition> implements SearchSpecification<VALUETYPE> {

	protected final String property;
	protected final CONDITIONTYPE condition;
	protected final VALUETYPE searchValue;

	protected AbstractSearchSpecification(final String property, final CONDITIONTYPE condition, final VALUETYPE searchValue) {
		checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkNotNull(searchValue, "Precondition violation - argument 'searchValue' must not be NULL!");
		this.property = property;
		this.condition = condition;
		this.searchValue = searchValue;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public String getProperty() {
		return this.property;
	}

	@Override
	public VALUETYPE getSearchValue() {
		return this.searchValue;
	}

	@Override
	public CONDITIONTYPE getCondition() {
		return this.condition;
	}

	// =================================================================================================================
	// HASH CODE & EQUALS
	// =================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.condition == null ? 0 : this.condition.hashCode());
		result = prime * result + (this.property == null ? 0 : this.property.hashCode());
		result = prime * result + (this.searchValue == null ? 0 : this.searchValue.hashCode());
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
		AbstractSearchSpecification<?, ?> other = (AbstractSearchSpecification<?, ?>) obj;
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
		return true;
	}

	@Override
	public String toString() {
		return this.getProperty() + " " + this.getCondition().getInfix() + " '" + this.getSearchValue() + "'";
	}
}
