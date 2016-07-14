package org.chronos.chronodb.internal.impl.temporal;

import static com.google.common.base.Preconditions.*;

import java.io.Serializable;

import com.google.common.base.Strings;

public class UnqualifiedTemporalKey implements Serializable, Comparable<UnqualifiedTemporalKey> {

	private static final char SEPARATOR = '@';

	public static UnqualifiedTemporalKey create(final String key, final long timestamp) {
		return new UnqualifiedTemporalKey(key, timestamp);
	}

	public static UnqualifiedTemporalKey createMin(final String key) {
		return new UnqualifiedTemporalKey(key, 0L);
	}

	public static UnqualifiedTemporalKey createMax(final String key) {
		return new UnqualifiedTemporalKey(key, Long.MAX_VALUE);
	}

	public static UnqualifiedTemporalKey createMin(final UnqualifiedTemporalKey key) {
		return new UnqualifiedTemporalKey(key.getKey(), 0L);
	}

	public static UnqualifiedTemporalKey createMax(final UnqualifiedTemporalKey key) {
		return new UnqualifiedTemporalKey(key.getKey(), Long.MAX_VALUE);
	}

	public static UnqualifiedTemporalKey parseSerializableFormat(final String serializedFormat) {
		checkNotNull(serializedFormat, "Precondition violation - argument 'serializedFormat' must not be NULL!");
		int separatorIndex = serializedFormat.lastIndexOf(SEPARATOR);
		if (separatorIndex == -1) {
			throw new IllegalArgumentException(
					"The string '" + serializedFormat + "' is no valid serial form of an UnqualifiedTemporalKey!");
		}
		try {
			String key = serializedFormat.substring(0, separatorIndex);
			String timestamp = serializedFormat.substring(separatorIndex + 1, serializedFormat.length());
			// remove leading zeroes
			timestamp = timestamp.replaceFirst("^0+(?!$)", "");
			long timestampValue = Long.parseLong(timestamp);
			return new UnqualifiedTemporalKey(key, timestampValue);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(
					"The string '" + serializedFormat + "' is no valid serial form of an UnqualifiedTemporalKey!", e);
		}
	}

	private final String key;
	private final long timestamp;

	public UnqualifiedTemporalKey(final String key, final long timestamp) {
		this.key = key;
		this.timestamp = timestamp;
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
		UnqualifiedTemporalKey other = (UnqualifiedTemporalKey) obj;
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
		builder.append("K['");
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

	@Override
	public int compareTo(final UnqualifiedTemporalKey o) {
		if (this.getKey().equals(o.getKey())) {
			if (this.getTimestamp() == o.getTimestamp()) {
				return 0;
			}
			if (this.getTimestamp() > o.getTimestamp()) {
				return 1;
			}
			return -1;
		}
		int comparisonValue = this.getKey().compareTo(o.getKey());
		if (comparisonValue < 0) {
			return -1;
		} else if (comparisonValue == 0) {
			return 0;
		} else {
			return 1;
		}
	}

	public String toSerializableFormat() {
		String timestampString = Strings.padStart(String.valueOf(this.timestamp), 19, '0');
		return this.key + SEPARATOR + timestampString;
	}
}