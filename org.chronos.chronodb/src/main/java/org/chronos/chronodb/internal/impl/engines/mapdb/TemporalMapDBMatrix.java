package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;
import static org.chronos.common.logging.ChronoLogger.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.mapdb.Serializer;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TemporalMapDBMatrix extends AbstractTemporalDataMatrix {

	private static final String INVERSE_MATRIX_SUFFIX = "_inv";

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final MapDBChronoDB db;
	private final String mapName;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public TemporalMapDBMatrix(final String keyspace, final long timestamp, final MapDBChronoDB db,
			final String matrixMapName) {
		super(keyspace, timestamp);
		checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
		checkNotNull(matrixMapName, "Precondition violation - argument 'matrixMapName' must not be NULL!");
		this.db = db;
		this.mapName = matrixMapName;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public byte[] get(final long time, final String key) {
		logTrace("[GET] keyspace = '" + this.getKeyspace() + "', key = '" + key + "', timestamp = " + time);
		String tkHigh = UnqualifiedTemporalKey.create(key, time).toSerializableFormat();
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			Entry<String, byte[]> entry = map.floorEntry(tkHigh);
			if (entry == null) {
				// there is no value for this key
				return null;
			}
			if (UnqualifiedTemporalKey.parseSerializableFormat(entry.getKey()).getKey().equals(key) == false) {
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
	}

	@Override
	public RangedGetResult<byte[]> getRanged(final long timestamp, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		logTrace("[GTR] keyspace = '" + this.getKeyspace() + "', key = '" + key + "', timestamp = " + timestamp);
		QualifiedKey qKey = QualifiedKey.create(this.getKeyspace(), key);
		String temporalKey = UnqualifiedTemporalKey.create(key, timestamp).toSerializableFormat();
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			Entry<String, byte[]> floorEntry = map.floorEntry(temporalKey);
			Entry<String, byte[]> ceilEntry = map.higherEntry(temporalKey);
			UnqualifiedTemporalKey floorKey = null;
			if (floorEntry != null) {
				floorKey = UnqualifiedTemporalKey.parseSerializableFormat(floorEntry.getKey());
			}
			UnqualifiedTemporalKey ceilKey = null;
			if (ceilEntry != null) {
				ceilKey = UnqualifiedTemporalKey.parseSerializableFormat(ceilEntry.getKey());
			}
			if (floorEntry == null || floorKey.getKey().equals(key) == false) {
				// we have no "next lower" bound -> we already know that the result will be empty.
				// now we need to check if we have an upper bound for the validity of our empty result...
				if (ceilEntry == null || ceilKey.getKey().equals(key) == false) {
					// there is no value for this key (at all, not at any timestamp)
					return RangedGetResult.createNoValueResult(qKey, Period.eternal());
				} else if (ceilEntry != null && ceilKey.getKey().equals(key)) {
					// there is no value for this key, until a certain timestamp is reached
					Period period = Period.createRange(0, ceilKey.getTimestamp());
					return RangedGetResult.createNoValueResult(qKey, period);
				}
			} else {
				// we have a "next lower" bound -> we already know that the result will be non-empty.
				// now we need to check if we have an upper bound for the validity of our result...
				if (ceilEntry == null || ceilKey.getKey().equals(key) == false) {
					// there is no further value for this key, therefore we have an open-ended period
					Period range = Period.createOpenEndedRange(floorKey.getTimestamp());
					byte[] value = floorEntry.getValue();
					if (value != null && value.length <= 0) {
						// value is non-null, but empty -> it's effectively null
						value = null;
					}
					return RangedGetResult.create(qKey, value, range);
				} else if (ceilEntry != null && ceilKey.getKey().equals(key)) {
					// the value of the result is valid between the floor and ceiling entries
					long floorTimestamp = floorKey.getTimestamp();
					long ceilTimestamp = ceilKey.getTimestamp();
					if (floorTimestamp >= ceilTimestamp) {
						ChronoLogger.logError("Invalid 'getRanged' state - floor timestamp (" + floorTimestamp
								+ ") >= ceil timestamp (" + ceilTimestamp + ")! Requested: '" + key + "@" + timestamp
								+ "', floor: '" + floorKey + "', ceil: '" + ceilKey + "'");
					}
					Period period = Period.createRange(floorKey.getTimestamp(), ceilKey.getTimestamp());
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

	}

	@Override
	public void put(final long time, final Map<String, byte[]> contents) {
		try (MapDBTransaction tx = this.openTransaction()) {
			Map<String, byte[]> map = this.getMap(tx);
			Map<String, Boolean> mapInverse = this.getMapInverse(tx);
			for (Entry<String, byte[]> entry : contents.entrySet()) {
				String key = entry.getKey();
				byte[] value = entry.getValue();
				UnqualifiedTemporalKey tk = UnqualifiedTemporalKey.create(key, time);
				if (value != null) {
					logTrace("[PUT] Key = '" + key + "', value = byte[" + value.length + "], timestamp = " + time);
					map.put(tk.toSerializableFormat(), value);
				} else {
					logTrace("[PUT] Key = '" + key + "', value = NULL, timestamp = " + time);
					map.put(tk.toSerializableFormat(), new byte[0]);
				}
				InverseUnqualifiedTemporalKey itk = InverseUnqualifiedTemporalKey.create(time, key);
				if (value != null) {
					mapInverse.put(itk.toSerializableFormat(), true);
				} else {
					mapInverse.put(itk.toSerializableFormat(), false);
				}
			}
			tx.commit();
		}
	}

	@Override
	public Iterator<String> keys(final long maxTime) {
		logTrace("[KEY] Retrieving key set on keyspace '" + this.getKeyspace() + "' at timestamp " + maxTime);
		List<String> keysDescending;
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			keysDescending = Lists.newArrayList(map.descendingKeySet());
		}
		return new KeysIterator(maxTime, keysDescending);
	}

	@Override
	public Iterator<String> allKeys() {
		logTrace("[AKY] Retrieving all keys on keyspace '" + this.getKeyspace() + "'");
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			Set<String> keys = map.keySet().stream()
					.map(key -> UnqualifiedTemporalKey.parseSerializableFormat(key).getKey())
					.collect(Collectors.toSet());
			return keys.iterator();
		}
	}

	@Override
	public Iterator<Long> history(final long maxTime, final String key) {
		logTrace("[HST] Retrieving history of key '" + key + "' in keyspace '" + this.getKeyspace() + "' at timestamp "
				+ maxTime);
		String tkMin = UnqualifiedTemporalKey.createMin(key).toSerializableFormat();
		String tkMax = UnqualifiedTemporalKey.create(key, maxTime).toSerializableFormat();
		List<String> keysDescending;
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			NavigableMap<String, byte[]> subMap = map.subMap(tkMin, true, tkMax, true);
			keysDescending = Lists.newArrayList(subMap.descendingKeySet());
		}
		Iterator<String> iterator = keysDescending.iterator();
		return new ChangeTimesIterator(iterator);
	}

	@Override
	public void insertEntries(final Set<UnqualifiedTemporalEntry> entries) {
		logTrace("[INS] Inserting " + entries.size() + " entries into keyspace '" + this.getKeyspace() + "'.");
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			for (UnqualifiedTemporalEntry entry : entries) {
				UnqualifiedTemporalKey key = entry.getKey();
				byte[] value = entry.getValue();
				map.put(key.toSerializableFormat(), value);
			}
			tx.commit();
		}
	}

	@Override
	public CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final long maxTimestamp) {
		logTrace("[AEN] Retrieving all entries in keyspace '" + this.getKeyspace() + "' before " + maxTimestamp);
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			return new AllEntriesIterator(tx, map, maxTimestamp);
		}
	}

	@Override
	public long lastCommitTimestamp(final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		logTrace(
				"[LCT] Retrieving last commit timestamp in keyspace '" + this.getKeyspace() + "' on key '" + key + "'");
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			String kMax = UnqualifiedTemporalKey.createMax(key).toSerializableFormat();
			String lastKey = map.floorKey(kMax);
			if (lastKey == null) {
				return -1;
			}
			UnqualifiedTemporalKey lastKeyParsed = UnqualifiedTemporalKey.parseSerializableFormat(lastKey);
			if (lastKeyParsed.getKey().equals(key) == false) {
				return -1;
			}
			return lastKeyParsed.getTimestamp();
		}
	}

	@Override
	public void rollback(final long timestamp) {
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<String, byte[]> map = this.getMap(tx);
			Iterator<Entry<String, byte[]>> iterator = map.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, byte[]> entry = iterator.next();
				UnqualifiedTemporalKey key = UnqualifiedTemporalKey.parseSerializableFormat(entry.getKey());
				if (key.getTimestamp() > timestamp) {
					iterator.remove();
				}
			}
			tx.commit();
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
		List<String> descendingKeySet = null;
		try (MapDBTransaction tx = this.openTransaction()) {
			String low = itkLow.toSerializableFormat();
			String high = itkHigh.toSerializableFormat();
			NavigableMap<String, Boolean> subMap = this.getMapInverse(tx).subMap(low, true, high, false);
			descendingKeySet = Lists.newArrayList(subMap.descendingKeySet());
		}
		Iterator<String> iterator = descendingKeySet.iterator();
		return Iterators.transform(iterator, (final String string) -> {
			InverseUnqualifiedTemporalKey itk = InverseUnqualifiedTemporalKey.parseSerializableFormat(string);
			return TemporalKey.create(itk.getTimestamp(), this.getKeyspace(), itk.getKey());
		});
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	public String getMapName() {
		return this.mapName;
	}

	protected MapDBTransaction openTransaction() {
		return this.db.openTransaction();
	}

	protected NavigableMap<String, byte[]> getMap(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.treeMap(this.mapName, Serializer.STRING, Serializer.BYTE_ARRAY);
	}

	protected NavigableMap<String, Boolean> getMapInverse(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.treeMap(this.mapName + INVERSE_MATRIX_SUFFIX, Serializer.STRING, Serializer.BOOLEAN);
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class KeysIterator implements Iterator<String> {

		private final long timeUpperBound;
		private final Set<String> returnedYs;
		private final Iterator<String> iterator;

		private String next;

		public KeysIterator(final long timeUpperBound, final List<String> descendingKeyList) {
			this.timeUpperBound = timeUpperBound;
			this.returnedYs = Sets.newHashSet();
			this.iterator = descendingKeyList.iterator();
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
				UnqualifiedTemporalKey tk = UnqualifiedTemporalKey.parseSerializableFormat(this.iterator.next());
				// ignore the key if the timestamp is after our transaction timestamp
				if (tk.getTimestamp() > this.timeUpperBound) {
					continue;
				}
				// ignore the key if we already returned it
				if (this.returnedYs.contains(tk.getKey())) {
					continue;
				}
				// ignore the key if the latest entry was a remove operation
				if (TemporalMapDBMatrix.this.get(this.timeUpperBound, tk.getKey()) == null) {
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

		private final Iterator<String> keyIterator;

		public ChangeTimesIterator(final Iterator<String> keyIterator) {
			this.keyIterator = keyIterator;
		}

		@Override
		public boolean hasNext() {
			return this.keyIterator.hasNext();
		}

		@Override
		public Long next() {
			return UnqualifiedTemporalKey.parseSerializableFormat(this.keyIterator.next()).getTimestamp();
		}

	}

	private static class AllEntriesIterator extends AbstractCloseableIterator<UnqualifiedTemporalEntry> {

		private final MapDBTransaction tx;
		private final Iterator<Entry<String, byte[]>> entryIterator;

		public AllEntriesIterator(final MapDBTransaction tx, final NavigableMap<String, byte[]> map,
				final long maxTimestamp) {
			checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
			checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
			checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
			this.tx = tx;
			this.entryIterator = map.entrySet().iterator();
		}

		@Override
		protected boolean hasNextInternal() {
			return this.entryIterator.hasNext();
		}

		@Override
		public UnqualifiedTemporalEntry next() {
			Entry<String, byte[]> entry = this.entryIterator.next();
			UnqualifiedTemporalKey key = UnqualifiedTemporalKey.parseSerializableFormat(entry.getKey());
			byte[] value = entry.getValue();
			return new UnqualifiedTemporalEntry(key, value);
		}

		@Override
		protected void closeInternal() {
			this.tx.close();
		}
	}
}
