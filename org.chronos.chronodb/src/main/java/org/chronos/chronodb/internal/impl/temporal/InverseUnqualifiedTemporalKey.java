package org.chronos.chronodb.internal.impl.temporal;

import static com.google.common.base.Preconditions.*;

import java.io.Serializable;

import com.google.common.base.Strings;

public class InverseUnqualifiedTemporalKey implements Serializable, Comparable<InverseUnqualifiedTemporalKey> {

	private static final char SEPARATOR = '#';

	public static InverseUnqualifiedTemporalKey create(final long timestamp, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new InverseUnqualifiedTemporalKey(timestamp, key);
	}

	public static InverseUnqualifiedTemporalKey createMinInclusive(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return new InverseUnqualifiedTemporalKey(timestamp, "");
	}

	public static InverseUnqualifiedTemporalKey createMaxExclusive(final long timestamp) {
		return new InverseUnqualifiedTemporalKey(timestamp + 1, "");
	}

	public static InverseUnqualifiedTemporalKey parseSerializableFormat(final String serializedFormat) {
		checkNotNull(serializedFormat, "Precondition violation - argument 'serializedFormat' must not be NULL!");
		int separatorIndex = serializedFormat.indexOf(SEPARATOR);
		if (separatorIndex == -1) {
			throw new IllegalArgumentException(
					"The string '" + serializedFormat + "' is no valid serial form of an InverseUnqualifiedTemporalKey!");
		}
		try {
			String key = serializedFormat.substring(separatorIndex + 1, serializedFormat.length());
			String timestamp = serializedFormat.substring(0, separatorIndex);
			// remove leading zeroes
			timestamp = timestamp.replaceFirst("^0+(?!$)", "");
			long timestampValue = Long.parseLong(timestamp);
			return new InverseUnqualifiedTemporalKey(timestampValue, key);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(
					"The string '" + serializedFormat + "' is no valid serial form of an InverseUnqualifiedTemporalKey!", e);
		}
	}

	private final String key;
	private final long timestamp;

	public InverseUnqualifiedTemporalKey(final long timestamp, final String key) {
		this.timestamp = timestamp;
		this.key = key;
	}

	public String getKey() {
		return this.key;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.key == null ? 0 : this.key.hashCode());
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
		InverseUnqualifiedTemporalKey other = (InverseUnqualifiedTemporalKey) obj;
		if (this.key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!this.key.equals(other.key)) {
			return false;
		}
		if (this.timestamp != other.timestamp) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("KI['");
		if (this.timestamp == Long.MAX_VALUE) {
			builder.append("MAX");
		} else if (this.timestamp <= 0) {
			builder.append("MIN");
		} else {
			builder.append(this.timestamp);
		}
		builder.append("'#");
		builder.append(this.getKey());
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int compareTo(final InverseUnqualifiedTemporalKey o) {
		if (this.getTimestamp() == o.getTimestamp()) {
			int comparisonValue = this.getKey().compareTo(o.getKey());
			if (comparisonValue < 0) {
				return -1;
			} else if (comparisonValue == 0) {
				return 0;
			} else {
				return 1;
			}

		} else if (this.getTimestamp() > o.getTimestamp()) {
			return 1;
		} else {
			return -1;
		}
	}

	public String toSerializableFormat() {
		String timestampString = Strings.padStart(String.valueOf(this.timestamp), 19, '0');
		return timestampString + SEPARATOR + this.key;
	}

}
