package org.chronos.chronodb.internal.impl.cache.mosaic;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.impl.cache.CacheStatisticsImpl;
import org.chronos.chronodb.internal.impl.cache.util.lru.UsageRegistry;

public class MosaicRow implements UsageRegistry.RemoveListener<GetResult<?>> {

	private final QualifiedKey rowKey;

	private final ReadWriteLock lock;

	private final UsageRegistry<GetResult<?>> lruRegistry;
	private final Set<GetResult<?>> contents;

	private final CacheStatisticsImpl statistics;

	private final RowSizeChangeCallback sizeChangeCallback;

	public MosaicRow(final QualifiedKey rowKey, final UsageRegistry<GetResult<?>> lruRegistry,
			final CacheStatisticsImpl statistics, final RowSizeChangeCallback callback) {
		checkNotNull(rowKey, "Precondition violation - argument 'rowKey' must not be NULL!");
		checkNotNull(lruRegistry, "Precondition violation - argument 'lruRegistry' must not be NULL!");
		this.rowKey = rowKey;
		this.lock = new ReentrantReadWriteLock(true);
		this.lruRegistry = lruRegistry;
		this.statistics = statistics;
		this.lruRegistry.addLeastRecentlyUsedRemoveListener(rowKey, this);
		this.contents = new ConcurrentSkipListSet<>(GetResultComparator.getInstance());
		this.sizeChangeCallback = callback;
	}

	@SuppressWarnings("unchecked")
	public <T> CacheGetResult<T> get(final long timestamp) {
		this.lock.readLock().lock();
		try {
			for (GetResult<?> result : this.contents) {
				Period range = result.getPeriod();
				if (range.contains(timestamp)) {
					// cache hit
					// remember in the LRU registry that we had a hit on this element
					this.lruRegistry.registerUsage(result);
					// convert into a cache-get-result
					T value = (T) result.getValue();
					this.statistics.registerHit();
					return CacheGetResult.hit(value, result.getPeriod().getLowerBound());
				}
			}
			// cache miss
			this.statistics.registerMiss();
			return CacheGetResult.miss();
		} finally {
			this.lock.readLock().unlock();
		}
	}

	/**
	 * Adds the given query result into this row.
	 *
	 * @param queryResult
	 *            The query result to add.
	 */
	public void put(final GetResult<?> queryResult) {
		// note: we really do need only the read lock here. The contents map can handle
		// this kind of concurrency easily without locking.
		boolean changed = false;
		this.lock.readLock().lock();
		try {
			changed = this.contents.add(queryResult);
		} finally {
			this.lock.readLock().unlock();
		}
		// remember in the LRU registry that this element was just added
		this.lruRegistry.registerUsage(queryResult);
		if (changed) {
			this.sizeChangeCallback.onRowSizeChanged(this, 1);
		}
	}

	/**
	 * Writes the given value through this row, at the given timestamp.
	 *
	 * <p>
	 * This method assumes that the given value will have a validity range that is open-ended on the righthand side.
	 *
	 * @param timestamp
	 *            The timestamp at which the write-through occurs. Must not be negative.
	 * @param value
	 *            The value to write. May be <code>null</code> to indicate a deletion.
	 */
	public void writeThrough(final long timestamp, final Object value) {
		// shorten the "valid to" period of the open-ended entry (if present) to the given timestamp
		this.limitOpenEndedPeriodEntryToUpperBound(timestamp);
		// create the new entry
		Period newPeriod = Period.createOpenEndedRange(timestamp);
		GetResult<?> newEntry = GetResult.create(this.rowKey, value, newPeriod);
		boolean changed = false;
		this.lock.readLock().lock();
		try {
			changed = this.contents.add(newEntry);
		} finally {
			this.lock.readLock().unlock();
		}
		this.lruRegistry.registerUsage(newEntry);
		if (changed) {
			this.sizeChangeCallback.onRowSizeChanged(this, 1);
		}
	}

	/**
	 * Rolls back this row to the specified timestamp, i.e. removes all entries with a validity range that is either
	 * after, or contains, the given timestamp.
	 *
	 * <p>
	 * This method will <b>not call</b> the {@link RowSizeChangeCallback}!
	 *
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative.
	 * @return The total number of elements of the cache entries that were removed due to this operation. May be zero,
	 *         but never negative.
	 */
	public int rollbackToTimestamp(final long timestamp) {
		this.lock.writeLock().lock();
		try {
			Iterator<GetResult<?>> iterator = this.contents.iterator();
			int totalRemovedElements = 0;
			while (iterator.hasNext()) {
				GetResult<?> entry = iterator.next();
				Period range = entry.getPeriod();
				if (range.isAfter(timestamp) || range.contains(timestamp)) {
					totalRemovedElements += 1;
					iterator.remove();
				}
			}
			return totalRemovedElements;
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	/**
	 * If this cache row contains an entry with a period that is open-ended on the righthand side, then this method
	 * limits the upper bound of this period to the given value.
	 *
	 * <p>
	 * If this row contains no entry with an open-ended period, this method has no effect.
	 *
	 * @param timestamp
	 *            The timestamp to use as the new upper bound for the validity period, in case that an entry with an
	 *            open-ended period exists. Must not be negative.
	 */
	public void limitOpenEndedPeriodEntryToUpperBound(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// get the first entry (highest in cache, latest period) and check if its range needs to be trimmed
		GetResult<?> firstEntry = this.contents.stream().findFirst().orElse(null);
		if (firstEntry != null && firstEntry.getPeriod().getUpperBound() > timestamp) {
			// the range of the entry needs to be trimmed to the current timestamp
			Period range = firstEntry.getPeriod();
			Period newRange = range.setUpperBound(timestamp);
			GetResult<?> replacementEntry = GetResult.create(this.rowKey, firstEntry.getValue(), newRange);
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
			this.lruRegistry.registerUsage(replacementEntry);
		}
	}

	public boolean isEmpty() {
		return this.contents.size() <= 0;
	}

	public int size() {
		return this.contents.size();
	}

	/**
	 * Returns the internal contents set.
	 *
	 * <p>
	 * This method is intended for testing purposes only. Do not modify the returned set!
	 *
	 * @return The internal contents set. Never <code>null</code>.
	 */
	public Set<GetResult<?>> getContents() {
		return Collections.unmodifiableSet(this.contents);
	}

	public void detach() {
		this.lruRegistry.removeLeastRecentlyUsedListener(this.rowKey, this);
	}

	@Override
	public void objectRemoved(final Object topic, final GetResult<?> element) {
		this.lock.writeLock().lock();
		try {
			int sizeBefore = this.size();
			boolean removed = this.contents.remove(element);
			if (removed == false) {
				return;
			}
			int sizeDelta = this.size() - sizeBefore;
			this.sizeChangeCallback.onRowSizeChanged(this, sizeDelta);
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	@FunctionalInterface
	public interface RowSizeChangeCallback {

		public void onRowSizeChanged(MosaicRow row, int sizeDeltaInElements);

	}

}
