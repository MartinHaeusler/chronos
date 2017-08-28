package org.chronos.chronodb.internal.impl.cache.bogus;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.cache.CacheStatisticsImpl;

public class ChronoDBBogusCache implements ChronoDBCache {

	@Override
	public <T> CacheGetResult<T> get(final String branch, final long timestamp, final QualifiedKey qualifiedKey) {
		return CacheGetResult.miss();
	}

	@Override
	public void cache(final String branch, final GetResult<?> queryResult) {
		// ignore
	}

	@Override
	public void writeThrough(final String branch, final long timestamp, final QualifiedKey key, final Object value) {
		// ignore
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public CacheStatistics getStatistics() {
		return new CacheStatisticsImpl();
	}

	@Override
	public void resetStatistics() {
		// ignore
	}

	@Override
	public void clear() {
		// ignore
	}

	@Override
	public void rollbackToTimestamp(final long timestamp) {
		// ignore
	}

}
