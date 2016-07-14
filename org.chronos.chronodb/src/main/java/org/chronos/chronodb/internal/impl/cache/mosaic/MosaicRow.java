package org.chronos.chronodb.internal.impl.cache.mosaic;

import static com.google.common.base.Preconditions.*;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.RangedGetResult;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.impl.cache.CacheStatisticsImpl;
import org.chronos.chronodb.internal.impl.cache.util.lru.UsageRegistry;

public class MosaicRow {

	private final QualifiedKey rowKey;

	private final ReadWriteLock lock;

	private final UsageRegistry<RangedGetResult<?>> lruRegistry;
	private final Set<RangedGetResult<?>> contents;

	private final CacheStatisticsImpl statistics;

	public MosaicRow(final QualifiedKey rowKey, final UsageRegistry<RangedGetResult<?>> lruRegistry,
			final CacheStatisticsImpl statistics) {
		checkNotNull(rowKey, "Precondition violation - argument 'rowKey' must not be NULL!");
		checkNotNull(lruRegistry, "Precondition violation - argument 'lruRegistry' must not be NULL!");
		this.rowKey = rowKey;
		this.lock = new ReentrantReadWriteLock(true);
		this.lruRegistry = lruRegistry;
		this.statistics = statistics;
		this.lruRegistry.addLeastRecentlyUsedRemoveListener(rowKey, this::onLeastRecentlyUsedEvict);
		this.contents = new ConcurrentSkipListSet<>(RangedGetResultComparator.getInstance());
	}

	@SuppressWarnings("unchecked")
	public <T> CacheGetResult<T> get(final long timestamp) {
		this.lock.readLock().lock();
		try {
			for (RangedGetResult<?> result : this.contents) {
				Period range = result.getRange();
				if (range.contains(timestamp)) {
					// cache hit
					// remember in the LRU registry that we had a hit on this element
					this.lruRegistry.registerUsage(result);
					// convert into a cache-get-result
					T value = (T) result.getValue();
					this.statistics.registerHit();
					return CacheGetResult.hit(value);
				}
			}
			// cache miss
			this.statistics.registerMiss();
			return CacheGetResult.miss();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	public void put(final RangedGetResult<?> queryResult) {
		// note: we really do need only the read lock here. The contents map can handle
		// this kind of concurrency easily without locking.
		this.lock.readLock().lock();
		try {
			this.contents.add(queryResult);
		} finally {
			this.lock.readLock().unlock();
		}
		// remember in the LRU registry that this element was just added
		this.lruRegistry.registerUsage(queryResult);
	}

	public void writeThrough(final long timestamp, final Object value) {
		// get the first entry (highest in cache, latest period) and check if its range needs to be trimmed
		RangedGetResult<?> firstEntry = this.contents.stream().findFirst().orElse(null);
		if (firstEntry != null && firstEntry.getRange().getUpperBound() > timestamp) {
			// the range of the entry needs to be trimmed to the current timestamp
			Period range = firstEntry.getRange();
			Period newRange = range.setUpperBound(timestamp);
			RangedGetResult<?> replacementEntry = RangedGetResult.create(this.rowKey, firstEntry.getValue(), newRange);
			this.lock.writeLock().lock();
			try {
				// note: we need the exclusive lock (write lock) here. The backing set determines equality
				// of entries based on the comparator which was passed to its constructor. That comparator
				// compares the validity ranges of the entries. Their compare(...) operation is based on the
				// lower bound of the period only. Furthermore, the backing set does NOT override an entry
				// on "set.add(...)" if the entry is already contained according to the comparator. Therefore,
				// we first need to remove the outdated entry, and then need to add the replacement. It is
				// crucial that this happens as an atomic operation - if another thread re-inserts the removed
				// outdated entry before we get the chance to install the replacement, then the outdated entry
				// will be "stuck" in the cache forever, placing the whole cache in an invalid state.
				this.contents.remove(firstEntry);
				this.contents.add(replacementEntry);
			} finally {
				this.lock.writeLock().unlock();
			}
		}
		// create the new entry
		Period newPeriod = Period.createOpenEndedRange(timestamp);
		RangedGetResult<?> newEntry = RangedGetResult.create(this.rowKey, value, newPeriod);
		this.lock.readLock().lock();
		try {
			this.contents.add(newEntry);
		} finally {
			this.lock.readLock().unlock();
		}
		this.lruRegistry.registerUsage(newEntry);
	}

	public void rollbackToTimestamp(final long timestamp) {
		this.lock.writeLock().lock();
		try {
			this.contents.removeIf(rangedGetResult -> {
				Period range = rangedGetResult.getRange();
				return range.isAfter(timestamp) || range.contains(timestamp);
			});
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	public int size() {
		return this.contents.size();
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private void onLeastRecentlyUsedEvict(final Object topic, final RangedGetResult<?> element) {
		this.contents.remove(element);
	}

	/**
	 * Returns the internal contents set.
	 *
	 * <p>
	 * This method is intended for testing purposes only. Do not modify the returned set!
	 *
	 * @return The internal contents set. Never <code>null</code>.
	 */
	protected Set<RangedGetResult<?>> getContents() {
		return this.contents;
	}

}
