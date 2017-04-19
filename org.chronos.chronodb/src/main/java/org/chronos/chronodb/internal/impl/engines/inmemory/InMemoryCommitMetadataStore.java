package org.chronos.chronodb.internal.impl.engines.inmemory;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.internal.impl.engines.base.AbstractCommitMetadataStore;
import org.chronos.chronodb.internal.util.NavigableMapUtils;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class InMemoryCommitMetadataStore extends AbstractCommitMetadataStore {

	private NavigableMap<Long, byte[]> commitMetadataMap;

	public InMemoryCommitMetadataStore(final ChronoDB owningDB, final Branch owningBranch) {
		super(owningDB, owningBranch);
		this.commitMetadataMap = new ConcurrentSkipListMap<>();
	}

	@Override
	protected byte[] getInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return this.commitMetadataMap.get(timestamp);
	}

	@Override
	protected void putInternal(final long commitTimestamp, final byte[] serializedMetadata) {
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		checkNotNull(serializedMetadata, "Precondition violation - argument 'serializedMetadata' must not be NULL!");
		this.commitMetadataMap.put(commitTimestamp, serializedMetadata);
	}

	@Override
	protected void rollbackToTimestampInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(timestamp, false, Long.MAX_VALUE, true);
		subMap.clear();
	}

	@Override
	public Iterator<Long> getCommitTimestampsBetween(final long from, final long to, final Order order) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		// fail-fast if the period is empty
		if (from > to) {
			return Collections.emptyIterator();
		}
		NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(from, true, to, true);
		switch (order) {
		case ASCENDING:
			// note: NavigableMap#keySet() is sorted in ascending order by default.
			return Iterators.unmodifiableIterator(subMap.keySet().iterator());
		case DESCENDING:
			return Iterators.unmodifiableIterator(subMap.descendingKeySet().iterator());
		default:
			throw new UnknownEnumLiteralException(order);
		}
	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final long from, final long to, final Order order) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		// fail-fast if the period is empty
		if (from > to) {
			return Collections.emptyIterator();
		}
		NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(from, true, to, true);
		final Iterator<Entry<Long, byte[]>> rawIterator;
		switch (order) {
		case ASCENDING:
			rawIterator = subMap.entrySet().iterator();
			break;
		case DESCENDING:
			rawIterator = subMap.descendingMap().entrySet().iterator();
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
		Iterator<Entry<Long, Object>> iterator = Iterators.transform(rawIterator,
				entry -> this.mapSerialEntryToPair(entry));
		return Iterators.unmodifiableIterator(iterator);
	}

	@Override
	public Iterator<Long> getCommitTimestampsPaged(final long minTimestamp, final long maxTimestamp, final int pageSize,
			final int pageIndex, final Order order) {
		checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
		checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
		checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
		checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		// fail-fast if the period is empty
		if (minTimestamp > maxTimestamp) {
			return Collections.emptyIterator();
		}
		int elementsToSkip = pageSize * pageIndex;
		NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(minTimestamp, true, maxTimestamp, true);
		final Iterator<Long> rawIterator;
		switch (order) {
		case ASCENDING:
			rawIterator = subMap.keySet().iterator();
			break;
		case DESCENDING:
			rawIterator = subMap.descendingKeySet().iterator();
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
		// skip entries of the iterator to arrive at the correct page
		for (int i = 0; i < elementsToSkip && rawIterator.hasNext(); i++) {
			rawIterator.next();
		}
		// limit the rest of the iterator to the given page size
		return Iterators.unmodifiableIterator(Iterators.limit(rawIterator, pageSize));
	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final long minTimestamp, final long maxTimestamp,
			final int pageSize, final int pageIndex, final Order order) {
		checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
		checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
		checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
		checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		// fail-fast if the period is empty
		if (minTimestamp > maxTimestamp) {
			return Collections.emptyIterator();
		}
		int elementsToSkip = pageSize * pageIndex;
		NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(minTimestamp, true, maxTimestamp, true);
		final Iterator<Entry<Long, byte[]>> rawIterator;
		switch (order) {
		case ASCENDING:
			rawIterator = subMap.entrySet().iterator();
			break;
		case DESCENDING:
			rawIterator = subMap.descendingMap().entrySet().iterator();
			break;
		default:
			throw new UnknownEnumLiteralException(order);
		}
		// skip entries of the iterator to arrive at the correct page
		for (int i = 0; i < elementsToSkip && rawIterator.hasNext(); i++) {
			rawIterator.next();
		}
		// convert the serialized commit metadata objects into their Object representation
		Iterator<Entry<Long, Object>> iterator = Iterators.transform(rawIterator,
				entry -> this.mapSerialEntryToPair(entry));
		// limit the rest of the iterator to the given page size
		return Iterators.unmodifiableIterator(Iterators.limit(iterator, pageSize));
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataAround(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		List<Entry<Long, byte[]>> entriesAround = NavigableMapUtils.entriesAround(this.commitMetadataMap, timestamp, count);
		List<Entry<Long, Object>> resultList = Lists.newArrayList();
		entriesAround.forEach(e -> resultList.add(this.deserializeValueOf(e)));
		resultList.sort(EntryTimestampComparator.INSTANCE.reversed());
		return resultList;
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataBefore(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		NavigableMap<Long, byte[]> map = this.commitMetadataMap.headMap(timestamp, false).descendingMap();
		List<Entry<Long, Object>> resultList = Lists.newArrayList();
		for (Entry<Long, byte[]> entry : map.entrySet()) {
			if (resultList.size() >= count) {
				break;
			}
			resultList.add(this.deserializeValueOf(entry));
		}
		return resultList;
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataAfter(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		NavigableMap<Long, byte[]> map = this.commitMetadataMap.tailMap(timestamp, false);
		List<Entry<Long, Object>> resultList = Lists.newArrayList();
		for (Entry<Long, byte[]> entry : map.entrySet()) {
			if (resultList.size() >= count) {
				break;
			}
			resultList.add(this.deserializeValueOf(entry));
		}
		// navigablemaps are sorted in ascending order, we want descending
		return Lists.reverse(resultList);
	}

	@Override
	public int countCommitTimestampsBetween(final long from, final long to) {
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		// fail-fast if the period is empty
		if (from >= to) {
			return 0;
		}
		NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(from, true, to, true);
		return subMap.size();
	}

	@Override
	public int countCommitTimestamps() {
		return this.commitMetadataMap.size();
	}

}
