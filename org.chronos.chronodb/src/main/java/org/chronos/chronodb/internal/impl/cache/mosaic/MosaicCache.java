package org.chronos.chronodb.internal.impl.cache.mosaic;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.cache.CacheStatisticsImpl;
import org.chronos.chronodb.internal.impl.cache.util.lru.FakeUsageRegistry;
import org.chronos.chronodb.internal.impl.cache.util.lru.RangedGetResultUsageRegistry;
import org.chronos.chronodb.internal.impl.cache.util.lru.UsageRegistry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MosaicCache implements ChronoDBCache {

	private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

	private final Map<String, Map<QualifiedKey, MosaicRow>> contents;
	private final UsageRegistry<GetResult<?>> lruRegistry;
	private final CacheStatisticsImpl statistics;

	private final int maxSize;
	private int currentSize;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public MosaicCache() {
		this(-1);
	}

	public MosaicCache(final int maxSize) {
		this.contents = new ConcurrentHashMap<>();
		this.maxSize = maxSize;
		if (this.maxSize <= 0) {
			this.lruRegistry = FakeUsageRegistry.getInstance();
		} else {
			this.lruRegistry = new RangedGetResultUsageRegistry();
		}
		this.currentSize = 0;
		this.statistics = new CacheStatisticsImpl();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public <T> CacheGetResult<T> get(final String branch, final long timestamp, final QualifiedKey qualifiedKey) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(qualifiedKey, "Precondition violation - argument 'qualifiedKey' must not be NULL!");
		this.lock.readLock().lock();
		try {
			Map<QualifiedKey, MosaicRow> qKeyToRow = this.contents.get(branch);
			if (qKeyToRow == null) {
				qKeyToRow = Maps.newConcurrentMap();
				this.contents.put(branch, qKeyToRow);
			}
			MosaicRow row = qKeyToRow.get(qualifiedKey);
			if (row == null) {
				this.statistics.registerMiss();
				return CacheGetResult.miss();
			}
			return row.get(timestamp);
		} finally {
			this.lock.readLock().unlock();
		}
	}

	@Override
	public void cache(final String branch, final GetResult<?> queryResult) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(queryResult, "Precondition violation - argument 'queryResult' must not be NULL!");
		this.lock.writeLock().lock();
		try {
			if (queryResult.getPeriod().isEmpty()) {
				// can't cache empty validity ranges
				return;
			}
			MosaicRow row = this.getOrCreateRow(branch, queryResult.getRequestedKey());
			row.put(queryResult);
			this.shrinkIfRequired();
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public void writeThrough(final String branch, final long timestamp, final QualifiedKey key, final Object value) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		this.lock.writeLock().lock();
		try {
			MosaicRow row = this.getOrCreateRow(branch, key);
			row.writeThrough(timestamp, value);
			this.shrinkIfRequired();
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public void rollbackToTimestamp(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		this.lock.writeLock().lock();
		try {
			for (Entry<String, Map<QualifiedKey, MosaicRow>> entry : this.contents.entrySet()) {
				Map<QualifiedKey, MosaicRow> qKeyToRow = entry.getValue();
				// keep a record of all rows that become empty due to the rollback, such that we can purge them
				// afterwards
				Set<QualifiedKey> keysWithEmptyRows = Sets.newHashSet();
				for (Entry<QualifiedKey, MosaicRow> entry2 : qKeyToRow.entrySet()) {
					QualifiedKey qKey = entry2.getKey();
					MosaicRow row = entry2.getValue();
					this.currentSize -= row.rollbackToTimestamp(timestamp);
					if (row.isEmpty()) {
						// remember to remove this map entry after we are done iterating
						keysWithEmptyRows.add(qKey);
					}
				}
				// purge empty entries in the inner map
				keysWithEmptyRows.forEach(qKey -> {
					qKeyToRow.get(qKey).detach();
					qKeyToRow.remove(qKey);
				});
			}
			// purge empty entries in the outer map
			this.contents.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public void limitAllOpenEndedPeriodsInBranchTo(final String branchName, final long timestamp) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.lock.writeLock().lock();
		try {
			Map<QualifiedKey, MosaicRow> qKeyToRow = this.contents.get(branchName);
			if (qKeyToRow != null) {
				for (Entry<QualifiedKey, MosaicRow> entry : qKeyToRow.entrySet()) {
					MosaicRow row = entry.getValue();
					row.limitOpenEndedPeriodEntryToUpperBound(timestamp);
				}
			}
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public void clear() {
		this.lock.writeLock().lock();
		try {
			// detach all rows
			this.contents.values().stream().flatMap(entry -> entry.values().stream()).forEach(row -> row.detach());
			this.contents.clear();
			this.lruRegistry.clear();
			this.currentSize = 0;
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public CacheStatistics getStatistics() {
		this.lock.readLock().lock();
		try {
			return this.statistics.duplicate();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	@Override
	public void resetStatistics() {
		this.lock.writeLock().lock();
		try {
			this.statistics.reset();
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public int size() {
		return this.currentSize;
	}

	@VisibleForTesting
	public int computedSize() {
		return this.contents.values().stream().flatMap(map -> map.values().stream()).mapToInt(row -> row.size()).sum();
	}

	@VisibleForTesting
	public int maxSize() {
		return this.maxSize;
	}

	@VisibleForTesting
	public int rowCount() {
		return (int) this.contents.values().stream().flatMap(map -> map.values().stream()).count();
	}

	@VisibleForTesting
	public int lruListenerCount() {
		return this.lruRegistry.getListenerCount();
	}

	@VisibleForTesting
	public int lruSize() {
		return this.lruRegistry.sizeInElements();
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private MosaicRow getOrCreateRow(final String branch, final QualifiedKey key) {
		Map<QualifiedKey, MosaicRow> qKeyToRow = this.contents.get(branch);
		if (qKeyToRow == null) {
			qKeyToRow = Maps.newConcurrentMap();
			this.contents.put(branch, qKeyToRow);
		}
		MosaicRow row = qKeyToRow.get(key);
		if (row == null) {
			// create a new row to hold the data
			row = new MosaicRow(key, this.lruRegistry, this.statistics, (r, delta) -> this.onRowSizeChanged(branch, key, r, delta));
			qKeyToRow.put(key, row);
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
		if (this.lruRegistry.sizeInElements() <= this.maxSize) {
			// we are still below the maximum allowed memory; no need to clean up
			return;
		}
		// note: removing the least recently used entry will trigger the callback chain, reducing
		// 'this.currentSizeBytes'.
		this.lruRegistry.removeLeastRecentlyUsedUntil(() -> this.lruRegistry.sizeInElements() <= this.maxSize);
	}

	protected void onRowSizeChanged(final String branch, final QualifiedKey key, final MosaicRow row, final long sizeDelta) {
		this.currentSize += sizeDelta;
		if (row.isEmpty()) {
			row.detach();
			Map<QualifiedKey, MosaicRow> qKeyToRow = this.contents.get(branch);
			if (qKeyToRow != null) {
				// remove the cache row
				qKeyToRow.remove(key);
				// it might be that the entire branch now has no more rows...
				if (qKeyToRow.isEmpty()) {
					this.contents.remove(branch);
				}
			}
		}
	}

}
