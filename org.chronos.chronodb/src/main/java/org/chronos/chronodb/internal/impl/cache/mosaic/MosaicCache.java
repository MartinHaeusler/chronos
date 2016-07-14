package org.chronos.chronodb.internal.impl.cache.mosaic;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.RangedGetResult;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.cache.CacheStatisticsImpl;
import org.chronos.chronodb.internal.impl.cache.util.lru.FakeUsageRegistry;
import org.chronos.chronodb.internal.impl.cache.util.lru.RangedGetResultUsageRegistry;
import org.chronos.chronodb.internal.impl.cache.util.lru.UsageRegistry;

public class MosaicCache implements ChronoDBCache {

	private final Map<QualifiedKey, MosaicRow> contents;
	private final UsageRegistry<RangedGetResult<?>> lruRegistry;
	private final int maxSize;

	private final CacheStatisticsImpl statistics;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public MosaicCache() {
		this(-1);
	}

	public MosaicCache(final int maxSize) {
		this.contents = new ConcurrentHashMap<>();
		this.statistics = new CacheStatisticsImpl();
		this.maxSize = maxSize;
		if (this.maxSize <= 0) {
			this.lruRegistry = FakeUsageRegistry.getInstance();
		} else {
			this.lruRegistry = new RangedGetResultUsageRegistry();
		}
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public <T> CacheGetResult<T> get(final long timestamp, final QualifiedKey qualifiedKey) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(qualifiedKey, "Precondition violation - argument 'qualifiedKey' must not be NULL!");
		MosaicRow row = this.contents.get(qualifiedKey);
		if (row == null) {
			this.statistics.registerMiss();
			return CacheGetResult.miss();
		}
		return row.get(timestamp);
	}

	@Override
	public void cache(final QualifiedKey key, final RangedGetResult<?> queryResult) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(queryResult, "Precondition violation - argument 'queryResult' must not be NULL!");
		if (queryResult.getRange().isEmpty()) {
			// can't cache empty validity ranges
			return;
		}
		MosaicRow row = this.getOrCreateRow(key);
		row.put(queryResult);
		this.shrinkIfRequired();
	}

	@Override
	public void writeThrough(final long timestamp, final QualifiedKey key, final Object value) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		MosaicRow row = this.getOrCreateRow(key);
		row.writeThrough(timestamp, value);
		this.shrinkIfRequired();
	}

	@Override
	public void rollbackToTimestamp(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		for (MosaicRow row : this.contents.values()) {
			row.rollbackToTimestamp(timestamp);
		}
	}

	@Override
	public void clear() {
		this.contents.clear();
		this.lruRegistry.clear();
	}

	@Override
	public CacheStatistics getStatistics() {
		return this.statistics;
	}

	@Override
	public int size() {
		return this.contents.values().stream().mapToInt(row -> row.size()).sum();
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private MosaicRow getOrCreateRow(final QualifiedKey key) {
		MosaicRow row = this.contents.get(key);
		if (row == null) {
			// create a new row to hold the data
			row = new MosaicRow(key, this.lruRegistry, this.statistics);
			this.contents.put(key, row);
		}
		return row;
	}

	protected boolean hasMaxSize() {
		return this.maxSize > 0;
	}

	protected void shrinkIfRequired() {
		if (this.hasMaxSize() == false) {
			// no max size given -> no need to shrink the size of the cache
			return;
		}
		this.lruRegistry.removeLeastRecentlyUsedUntilSizeIs(this.maxSize);
	}

}
