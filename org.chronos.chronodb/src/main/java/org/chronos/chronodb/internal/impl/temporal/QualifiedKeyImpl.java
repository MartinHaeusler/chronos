package org.chronos.chronodb.internal.impl.temporal;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.key.QualifiedKey;

public final class QualifiedKeyImpl implements QualifiedKey {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String key;
	private String keyspace;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected QualifiedKeyImpl() {
		// default constructor for serialization purposes
	}

	public QualifiedKeyImpl(final String keyspace, final String key) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		this.key = key;
		this.keyspace = keyspace;
	}

	// =====================================================================================================================
	// GETTERS
	// =====================================================================================================================

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String getKeyspace() {
		return this.keyspace;
	}

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.key == null ? 0 : this.key.hashCode());
		result = prime * result + (this.keyspace == null ? 0 : this.keyspace.hashCode());
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
		QualifiedKeyImpl other = (QualifiedKeyImpl) obj;
		if (this.key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!this.key.equals(other.key)) {
			return false;
		}
		if (this.keyspace == null) {
			if (other.keyspace != null) {
				return false;
			}
		} else if (!this.keyspace.equals(other.keyspace)) {
			return false;
		}
		return true;
	}

	// =====================================================================================================================
	// TOSTRING
	// =====================================================================================================================

	@Override
	public String toString() {
		return "QK['" + this.keyspace + "->" + this.key + "']";
	}
}
