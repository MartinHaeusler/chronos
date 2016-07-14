package org.chronos.chronodb.internal.api.cache;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Map.Entry;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.RangedGetResult;

public interface ChronoDBCache {

	// =====================================================================================================================
	// BASIC METHODS
	// =====================================================================================================================

	public <T> CacheGetResult<T> get(long timestamp, QualifiedKey qualifiedKey);

	public void cache(QualifiedKey key, RangedGetResult<?> queryResult);

	public void writeThrough(long timestamp, QualifiedKey key, Object value);

	public int size();

	public CacheStatistics getStatistics();

	public void clear();

	public void rollbackToTimestamp(long timestamp);

	// =====================================================================================================================
	// EXTENSION METHODS
	// =====================================================================================================================

	public default void writeThrough(final long timestamp, final Map<QualifiedKey, Object> keyValues) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(keyValues, "Precondition violation - argument 'keyValues' must not be NULL!");
		for (Entry<QualifiedKey, Object> entry : keyValues.entrySet()) {
			this.writeThrough(timestamp, entry.getKey(), entry.getValue());
		}
	}

	// =====================================================================================================================
	// INNER INTERFACES
	// =====================================================================================================================

	public interface CacheStatistics {

		public long getCacheHitCount();

		public long getCacheMissCount();

		public void reset();

		public default long getRequestCount() {
			return getCacheHitCount() + getCacheMissCount();
		}

		public default double getCacheHitRatio() {
			double requestCount = this.getRequestCount();
			if (requestCount == 0) {
				// by definition, if we have no requests, we have no hit ratio
				return Double.NaN;
			}
			return getCacheHitCount() / requestCount;
		}

		public default double getCacheMissRatio() {
			double requestCount = this.getRequestCount();
			if (requestCount == 0) {
				// by definition, if we have no requests, we have no miss ratio
				return Double.NaN;
			}
			return getCacheMissCount() / requestCount;
		}

	}

}
