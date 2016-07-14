package org.chronos.chronodb.internal.impl.cache;

import java.util.concurrent.atomic.AtomicLong;

import org.chronos.chronodb.internal.api.cache.ChronoDBCache.CacheStatistics;

public class CacheStatisticsImpl implements CacheStatistics {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private AtomicLong hitCount;
	private AtomicLong missCount;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public CacheStatisticsImpl() {
		this.hitCount = new AtomicLong(0L);
		this.missCount = new AtomicLong(0L);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public long getCacheHitCount() {
		return this.hitCount.get();
	}

	@Override
	public long getCacheMissCount() {
		return this.missCount.get();
	}

	@Override
	public void reset() {
		this.hitCount.set(0L);
		this.missCount.set(0L);
	}

	// =====================================================================================================================
	// INTERNAL API
	// =====================================================================================================================

	public void registerHit() {
		this.hitCount.incrementAndGet();
	}

	public void registerMiss() {
		this.missCount.incrementAndGet();
	}

	// =====================================================================================================================
	// TO STRING
	// =====================================================================================================================

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("CacheStatistics[Hit: ");
		builder.append(this.getCacheHitCount());
		builder.append(", Miss: ");
		builder.append(this.getCacheMissCount());
		builder.append(", Hit Ratio: ");
		builder.append(this.getCacheHitRatio() * 100.0);
		builder.append("%]");
		return builder.toString();
	}

}
