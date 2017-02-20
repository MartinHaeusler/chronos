package org.chronos.chronodb.internal.util;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.common.logging.ChronoLogger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DataMatrixUtil {

	public static GetResult<byte[]> get(final NavigableMap<String, byte[]> map, final String keyspace,
			final long timestamp, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		logTrace("[GTR] keyspace = '" + keyspace + "', key = '" + key + "', timestamp = " + timestamp);
		QualifiedKey qKey = QualifiedKey.create(keyspace, key);
		String temporalKey = UnqualifiedTemporalKey.create(key, timestamp).toSerializableFormat();
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
				return GetResult.createNoValueResult(qKey, Period.eternal());
			} else if (ceilEntry != null && ceilKey.getKey().equals(key)) {
				// there is no value for this key, until a certain timestamp is reached
				Period period = Period.createRange(0, ceilKey.getTimestamp());
				return GetResult.createNoValueResult(qKey, period);
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
				return GetResult.create(qKey, value, range);
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
				return GetResult.create(qKey, value, period);
			}
		}
		// this code is effectively unreachable
		throw new RuntimeException("Unreachable code has been reached!");
	}

	public static void put(final NavigableMap<String, byte[]> map, final NavigableMap<String, Boolean> inverseMap,
			final String keyspace, final long time, final Map<String, byte[]> contents) {
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		checkNotNull(inverseMap, "Precondition violation - argument 'inverseMap' must not be NULL!");
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
				inverseMap.put(itk.toSerializableFormat(), true);
			} else {
				inverseMap.put(itk.toSerializableFormat(), false);
			}
		}
	}

	public static KeySetModifications keySetModifications(final NavigableMap<String, byte[]> map, final String keyspace,
			final long timestamp) {
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// entry set is sorted in ascending order!
		Set<Entry<String, byte[]>> entrySet = map.entrySet();
		Set<String> additions = Sets.newHashSet();
		Set<String> removals = Sets.newHashSet();
		// iterate over the full B-Tree key set (ascending order)
		Iterator<Entry<String, byte[]>> allEntriesIterator = entrySet.iterator();
		while (allEntriesIterator.hasNext()) {
			Entry<String, byte[]> currentEntry = allEntriesIterator.next();
			UnqualifiedTemporalKey currentKey = UnqualifiedTemporalKey.parseSerializableFormat(currentEntry.getKey());
			if (currentKey.getTimestamp() > timestamp) {
				continue;
			}
			String plainKey = currentKey.getKey();
			if (currentEntry.getValue() == null || currentEntry.getValue().length <= 0) {
				// removal
				additions.remove(plainKey);
				removals.add(plainKey);
			} else {
				// put
				additions.add(plainKey);
				removals.remove(plainKey);
			}
		}
		return new KeySetModifications(additions, removals);
	}

	public static Iterator<Long> history(final NavigableMap<String, byte[]> map, final String keyspace,
			final long maxTime, final String key) {
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(maxTime >= 0, "Precondition violation - argument 'maxTime' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		logTrace(
				"[HST] Retrieving history of key '" + key + "' in keyspace '" + keyspace + "' at timestamp " + maxTime);
		String tkMin = UnqualifiedTemporalKey.createMin(key).toSerializableFormat();
		String tkMax = UnqualifiedTemporalKey.create(key, maxTime).toSerializableFormat();
		NavigableMap<String, byte[]> subMap = map.subMap(tkMin, true, tkMax, true);
		List<Long> timestamps = subMap.descendingKeySet().stream()
				.map(stringKey -> UnqualifiedTemporalKey.parseSerializableFormat(stringKey).getTimestamp())
				.collect(Collectors.toList());
		return timestamps.iterator();
	}

	public static void insertEntries(final NavigableMap<String, byte[]> map, final String keyspace,
			final Set<UnqualifiedTemporalEntry> entries) {
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
		logTrace("[INS] Inserting " + entries.size() + " entries into keyspace '" + keyspace + "'.");
		for (UnqualifiedTemporalEntry entry : entries) {
			UnqualifiedTemporalKey key = entry.getKey();
			byte[] value = entry.getValue();
			map.put(key.toSerializableFormat(), value);
		}
	}

	public static long lastCommitTimestamp(final NavigableMap<String, byte[]> map, final String keyspace,
			final String key) {
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		logTrace("[LCT] Retrieving last commit timestamp in keyspace '" + keyspace + "' on key '" + key + "'");
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

	public static void rollback(final NavigableMap<String, byte[]> map, final NavigableMap<String, Boolean> inverseMap,
			final long timestamp) {
		checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
		checkNotNull(inverseMap, "Precondition violation - argument 'inverseMap' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		Iterator<Entry<String, byte[]>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, byte[]> entry = iterator.next();
			UnqualifiedTemporalKey key = UnqualifiedTemporalKey.parseSerializableFormat(entry.getKey());
			if (key.getTimestamp() > timestamp) {
				iterator.remove();
			}
		}
		Iterator<Entry<String, Boolean>> iterator2 = inverseMap.entrySet().iterator();
		while (iterator2.hasNext()) {
			Entry<String, Boolean> entry = iterator2.next();
			InverseUnqualifiedTemporalKey key = InverseUnqualifiedTemporalKey.parseSerializableFormat(entry.getKey());
			if (key.getTimestamp() > timestamp) {
				iterator2.remove();
			}
		}
	}

	public static Iterator<TemporalKey> getModificationsBetween(final NavigableMap<String, Boolean> inverseMap,
			final String keyspace, final long timestampLowerBound, final long timestampUpperBound) {
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		InverseUnqualifiedTemporalKey itkLow = InverseUnqualifiedTemporalKey.createMinInclusive(timestampLowerBound);
		InverseUnqualifiedTemporalKey itkHigh = InverseUnqualifiedTemporalKey.createMaxExclusive(timestampUpperBound);
		List<String> descendingKeySet = null;
		String low = itkLow.toSerializableFormat();
		String high = itkHigh.toSerializableFormat();
		NavigableMap<String, Boolean> subMap = inverseMap.subMap(low, true, high, false);
		descendingKeySet = Lists.newArrayList(subMap.descendingKeySet());
		List<TemporalKey> resultList = Lists.newArrayList();
		for (String string : descendingKeySet) {
			InverseUnqualifiedTemporalKey itk = InverseUnqualifiedTemporalKey.parseSerializableFormat(string);
			resultList.add(TemporalKey.create(itk.getTimestamp(), keyspace, itk.getKey()));
		}
		return resultList.iterator();
	}

}
