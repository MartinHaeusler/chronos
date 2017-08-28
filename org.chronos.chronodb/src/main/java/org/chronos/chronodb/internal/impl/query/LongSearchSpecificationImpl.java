package org.chronos.chronodb.internal.impl.query;

import java.util.function.Predicate;

import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

public class LongSearchSpecificationImpl extends AbstractSearchSpecification<Long, NumberCondition> implements LongSearchSpecification {

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public LongSearchSpecificationImpl(final String property, final NumberCondition condition, final Long searchValue) {
		super(property, condition, searchValue);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public Predicate<Object> toFilterPredicate() {
		return (obj) -> {
			if (obj instanceof Long == false) {
				return false;
			}
			long value = (long) obj;
			return this.condition.applies(value, this.searchValue);
		};
	}

	@Override
	public SearchSpecification<Long> negate() {
		return new LongSearchSpecificationImpl(this.getProperty(), this.getCondition().negate(), this.getSearchValue());
	}

	// =================================================================================================================
	// HASH CODE & EQUALS
	// =================================================================================================================

	@Override
	public int hashCode() {
		return super.hashCode();
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
		LongSearchSpecificationImpl other = (LongSearchSpecificationImpl) obj;
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
}
