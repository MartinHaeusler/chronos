package org.chronos.chronograph.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.api.index.ChronoGraphIndex;

public abstract class AbstractChronoGraphIndex implements ChronoGraphIndex {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String indexedProperty;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected AbstractChronoGraphIndex() {
		// default constructor for serialization
	}

	public AbstractChronoGraphIndex(final String indexedProperty) {
		checkNotNull(indexedProperty, "Precondition violation - argument 'indexedProperty' must not be NULL!");
		this.indexedProperty = indexedProperty;
	}

	@Override
	public String getIndexedProperty() {
		return this.indexedProperty;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.indexedProperty == null ? 0 : this.indexedProperty.hashCode());
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
		AbstractChronoGraphIndex other = (AbstractChronoGraphIndex) obj;
		if (this.indexedProperty == null) {
			if (other.indexedProperty != null) {
				return false;
			}
		} else if (!this.indexedProperty.equals(other.indexedProperty)) {
			return false;
		}
		return true;
	}

}
