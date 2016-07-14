package org.chronos.chronodb.internal.impl.engines.inmemory;

import static com.google.common.base.Preconditions.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.RangedGetResult;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalDataMatrix;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.common.logging.ChronoLogger;

import com.google.common.collect.Iterators;

public class TemporalInMemoryMatrix extends AbstractTemporalDataMatrix {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final NavigableMap<UnqualifiedTemporalKey, byte[]> contents;
	private final NavigableMap<InverseUnqualifiedTemporalKey, Boolean> inverseContents;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public TemporalInMemoryMatrix(final String keyspace, final long timestamp) {
		super(keyspace, timestamp);
		this.contents = new ConcurrentSkipListMap<UnqualifiedTemporalKey, byte[]>();
		this.inverseContents = new ConcurrentSkipListMap<InverseUnqualifiedTemporalKey, Boolean>();
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public byte[] get(final long time, final String key) {
		UnqualifiedTemporalKey tkHigh = UnqualifiedTemporalKey.create(key, time);
		Entry<UnqualifiedTemporalKey, byte[]> entry = this.contents.floorEntry(tkHigh);
		if (entry == null) {
			// there is no value for this key
			return null;
		}
		if (entry.getKey().getKey().equals(key) == false) {
			// no value for this key at the given timestamp
			return null;
		}
		byte[] value = entry.getValue();
		if (value == null || value.length <= 0) {
			return null;
		} else {
			return value;
		}
	}

	@Override
	public RangedGetResult<byte[]> getRanged(final long timestamp, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		QualifiedKey qKey = QualifiedKey.create(this.getKeyspace(), key);
		UnqualifiedTemporalKey temporalKey = UnqualifiedTemporalKey.create(key, timestamp);
		Entry<UnqualifiedTemporalKey, byte[]> floorEntry = this.contents.floorEntry(temporalKey);
		Entry<UnqualifiedTemporalKey, byte[]> ceilEntry = this.contents.higherEntry(temporalKey);
		if (floorEntry == null || floorEntry.getKey().getKey().equals(key) == false) {
			// we have no "next lower" bound -> we already know that the result will be empty.
			// now we need to check if we have an upper bound for the validity of our empty result...
			if (ceilEntry == null || ceilEntry.getKey().getKey().equals(key) == false) {
				// there is no value for this key (at all, not at any timestamp)
				return RangedGetResult.createNoValueResult(qKey, Period.eternal());
			} else if (ceilEntry != null && ceilEntry.getKey().getKey().equals(key)) {
				// there is no value for this key, until a certain timestamp is reached
				Period period = Period.createRange(0, ceilEntry.getKey().getTimestamp());
				return RangedGetResult.createNoValueResult(qKey, period);
			}
		} else {
			// we have a "next lower" bound -> we already know that the result will be non-empty.
			// now we need to check if we have an upper bound for the validity of our result...
			if (ceilEntry == null || ceilEntry.getKey().getKey().equals(key) == false) {
				// there is no further value for this key, therefore we have an open-ended period
				Period range = Period.createOpenEndedRange(floorEntry.getKey().getTimestamp());
				byte[] value = floorEntry.getValue();
				if (value != null && value.length <= 0) {
					// value is non-null, but empty -> it's effectively null
					value = null;
				}
				return RangedGetResult.create(qKey, value, range);
			} else if (ceilEntry != null && ceilEntry.getKey().getKey().equals(key)) {
				// the value of the result is valid between the floor and ceiling entries
				Period period = Period.createRange(floorEntry.getKey().getTimestamp(),
						ceilEntry.getKey().getTimestamp());
				byte[] value = floorEntry.getValue();
				if (value != null && value.length <= 0) {
					// value is non-null, but empty -> it's effectively null
					value = null;
				}
				return RangedGetResult.create(qKey, value, period);
			}
		}
		// this code is effectively unreachable
		throw new RuntimeException("Unreachable code has been reached!");
	}

	@Override
	public void put(final long time, final Map<String, byte[]> contents) {
		for (Entry<String, byte[]> entry : contents.entrySet()) {
			String key = entry.getKey();
			byte[] value = entry.getValue();
			UnqualifiedTemporalKey tk = UnqualifiedTemporalKey.create(key, time);
			if (value != null) {
				ChronoLogger.logTrace("[PUT] " + tk + "bytes[" + value.length + "]");
				this.contents.put(tk, value);
			} else {
				this.contents.put(tk, new byte[0]);
				ChronoLogger.logTrace("[PUT] " + tk + "NULL");
			}
			InverseUnqualifiedTemporalKey itk = InverseUnqualifiedTemporalKey.create(time, key);
			if (value != null) {
				this.inverseContents.put(itk, true);
			} else {
				this.inverseContents.put(itk, false);
			}
		}
	}

	@Override
	public Iterator<String> keys(final long timestamp) {
		return new KeysIterator(timestamp);
	}

	@Override
	public Iterator<String> allKeys() {
		return Iterators.transform(this.contents.descendingKeySet().iterator(), uqKey -> uqKey.getKey());
	}

	@Override
	public Iterator<Long> history(final long maxTime, final String key) {
		checkArgument(maxTime >= 0, "Precondition violation - argument 'maxTime' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		UnqualifiedTemporalKey tkMin = UnqualifiedTemporalKey.createMin(key);
		UnqualifiedTemporalKey tkMax = UnqualifiedTemporalKey.create(key, maxTime);
		NavigableMap<UnqualifiedTemporalKey, byte[]> subMap = this.contents.subMap(tkMin, true, tkMax, true);
		Iterator<UnqualifiedTemporalKey> iterator = subMap.descendingKeySet().iterator();
		return new ChangeTimesIterator(iterator);
	}

	@Override
	public void insertEntries(final Set<UnqualifiedTemporalEntry> entries) {
		for (UnqualifiedTemporalEntry entry : entries) {
			UnqualifiedTemporalKey key = entry.getKey();
			byte[] value = entry.getValue();
			this.contents.put(key, value);
		}
	}

	@Override
	public CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final long timestamp) {
		return new AllEntriesIterator(this.contents.entrySet().iterator(), timestamp);
	}

	@Override
	public long lastCommitTimestamp(final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		UnqualifiedTemporalKey kMax = UnqualifiedTemporalKey.createMax(key);
		UnqualifiedTemporalKey lastKey = this.contents.floorKey(kMax);
		if (lastKey == null || lastKey.getKey().equals(key) == false) {
			return -1;
		}
		return lastKey.getTimestamp();
	}

	@Override
	public void rollback(final long timestamp) {
		Iterator<Entry<UnqualifiedTemporalKey, byte[]>> iterator = this.contents.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<UnqualifiedTemporalKey, byte[]> entry = iterator.next();
			if (entry.getKey().getTimestamp() > timestamp) {
				iterator.remove();
			}
		}
	}

	@Override
	public Iterator<TemporalKey> getModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		InverseUnqualifiedTemporalKey itkLow = InverseUnqualifiedTemporalKey.createMinInclusive(timestampLowerBound);
		InverseUnqualifiedTemporalKey itkHigh = InverseUnqualifiedTemporalKey.createMaxExclusive(timestampUpperBound);
		NavigableMap<InverseUnqualifiedTemporalKey, Boolean> subMap = this.inverseContents.subMap(itkLow, true, itkHigh,
				false);
		Iterator<InverseUnqualifiedTemporalKey> iterator = subMap.keySet().iterator();
		return Iterators.transform(iterator,
				itk -> TemporalKey.create(itk.getTimestamp(), this.getKeyspace(), itk.getKey()));
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class KeysIterator implements Iterator<String> {

		private final long timestamp;
		private final Set<String> returnedYs;
		private final Iterator<UnqualifiedTemporalKey> iterator;

		private String next;

		public KeysIterator(final long timestamp) {
			this.timestamp = timestamp;
			this.returnedYs = new HashSet<>();
			this.iterator = TemporalInMemoryMatrix.this.contents.descendingKeySet().iterator();
		}

		@Override
		public boolean hasNext() {
			if (this.next != null) {
				return true;
			}
			if (this.iterator.hasNext() == false) {
				return false;
			}
			while (this.iterator.hasNext()) {
				UnqualifiedTemporalKey tk = this.iterator.next();
				// ignore the key if the timestamp is after our transaction timestamp
				if (tk.getTimestamp() > this.timestamp) {
					continue;
				}
				// ignore the key if we already returned it
				if (this.returnedYs.contains(tk.getKey())) {
					continue;
				}
				// ignore the key if the latest entry was a remove operation
				if (TemporalInMemoryMatrix.this.get(this.timestamp, tk.getKey()) == null) {
					continue;
				}
				this.next = tk.getKey();
				this.returnedYs.add(tk.getKey());
				return true;
			}
			this.next = null;
			return false;
		}

		@Override
		public String next() {
			String result = this.next;
			this.next = null;
			return result;
		}

	}

	private static class ChangeTimesIterator implements Iterator<Long> {

		private final Iterator<UnqualifiedTemporalKey> keyIterator;

		public ChangeTimesIterator(final Iterator<UnqualifiedTemporalKey> keyIterator) {
			this.keyIterator = keyIterator;
		}

		@Override
		public boolean hasNext() {
			return this.keyIterator.hasNext();
		}

		@Override
		public Long next() {
			return this.keyIterator.next().getTimestamp();
		}

	}

	private static class AllEntriesIterator extends AbstractCloseableIterator<UnqualifiedTemporalEntry> {

		private final Iterator<Entry<UnqualifiedTemporalKey, byte[]>> entryIterator;

		public AllEntriesIterator(final Iterator<Entry<UnqualifiedTemporalKey, byte[]>> entryIterator,
				final long maxTimestamp) {
			this.entryIterator = Iterators.filter(entryIterator,
					entry -> entry.getKey().getTimestamp() <= maxTimestamp);
		}

		@Override
		protected boolean hasNextInternal() {
			return this.entryIterator.hasNext();
		}

		@Override
		public UnqualifiedTemporalEntry next() {
			Entry<UnqualifiedTemporalKey, byte[]> entry = this.entryIterator.next();
			UnqualifiedTemporalKey key = entry.getKey();
			byte[] value = entry.getValue();
			return new UnqualifiedTemporalEntry(key, value);
		}

		@Override
		protected void closeInternal() {
			// nothing to do for an in-memory matrix.
		}
	}

}
