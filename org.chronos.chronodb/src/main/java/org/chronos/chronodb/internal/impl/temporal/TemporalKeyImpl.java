package org.chronos.chronodb.internal.impl.temporal;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;

public final class TemporalKeyImpl /* NOT extends QualifiedKeyImpl */ implements TemporalKey {

	private long timestamp;
	private String keyspace;
	private String key;

	protected TemporalKeyImpl() {
		// default constructor for serialization purposes
	}

	public TemporalKeyImpl(final long timestamp, final String keyspace, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		this.timestamp = timestamp;
		this.keyspace = keyspace;
		this.key = key;
	}

	// =====================================================================================================================
	// GETTERS
	// =====================================================================================================================

	@Override
	public long getTimestamp() {
		return this.timestamp;
	}

	@Override
	public String getKeyspace() {
		return this.keyspace;
	}

	@Override
	public String getKey() {
		return this.key;
	}

	// =====================================================================================================================
	// CONVERSION METHODS
	// =====================================================================================================================

	@Override
	public QualifiedKey toQualifiedKey() {
		return QualifiedKey.create(this.keyspace, this.key);
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
		result = prime * result + (int) (this.timestamp ^ this.timestamp >>> 32);
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
		TemporalKeyImpl other = (TemporalKeyImpl) obj;
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
		if (this.timestamp != other.timestamp) {
			return false;
		}
		return true;
	}

	// =====================================================================================================================
	// TOSTRING
	// =====================================================================================================================

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TK['");
		builder.append(this.getKeyspace());
		builder.append("->");
		builder.append(this.getKey());
		builder.append("'@");
		if (this.timestamp == Long.MAX_VALUE) {
			builder.append("MAX");
		} else if (this.timestamp <= 0) {
			builder.append("MIN");
		} else {
			builder.append(this.timestamp);
		}
		builder.append("]");
		return builder.toString();
	}

	// =====================================================================================================================
	// COMPARISON
	// =====================================================================================================================

	// @Override
	// public int compareTo(final TemporalKey o) {
	// if (this.getKey().equals(o.getKey())) {
	// if (this.getTimestamp() == o.getTimestamp()) {
	// return 0;
	// }
	// if (this.getTimestamp() > o.getTimestamp()) {
	// return 1;
	// }
	// return -1;
	// }
	// int comparisonValue = this.getKey().compareTo(o.getKey());
	// if (comparisonValue < 0) {
	// return -1;
	// } else if (comparisonValue == 0) {
	// return 0;
	// } else {
	// return 1;
	// }
	// }
}
