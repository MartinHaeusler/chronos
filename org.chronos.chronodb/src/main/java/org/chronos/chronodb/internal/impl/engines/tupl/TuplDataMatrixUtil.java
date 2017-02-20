package org.chronos.chronodb.internal.impl.engines.tupl;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.chronodb.internal.util.KeySetModifications;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.cojen.tupl.Cursor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TuplDataMatrixUtil {

	private static final String INVERSE_MATRIX_SUFFIX = "_inv";

	public static GetResult<byte[]> get(final TuplTransaction tx, final String indexName, final String keyspace,
			final long timestamp, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		logTrace("[GTR] keyspace = '" + keyspace + "', key = '" + key + "', timestamp = " + timestamp);
		QualifiedKey qKey = QualifiedKey.create(keyspace, key);
		Pair<Entry<String, byte[]>, Entry<String, byte[]>> floorAndHigherEntry = floorEntryAndHigherEntry(tx, indexName,
				key, timestamp);
		Entry<String, byte[]> floorEntry = floorAndHigherEntry.getLeft();
		Entry<String, byte[]> higherEntry = floorAndHigherEntry.getRight();
		UnqualifiedTemporalKey floorKey = null;
		if (floorEntry != null) {
			floorKey = UnqualifiedTemporalKey.parseSerializableFormat(floorEntry.getKey());
		}
		UnqualifiedTemporalKey ceilKey = null;
		if (higherEntry != null) {
			ceilKey = UnqualifiedTemporalKey.parseSerializableFormat(higherEntry.getKey());
		}
		if (floorEntry == null || floorKey.getKey().equals(key) == false) {
			// we have no "next lower" bound -> we already know that the result will be empty.
			// now we need to check if we have an upper bound for the validity of our empty result...
			if (higherEntry == null || ceilKey.getKey().equals(key) == false) {
				// there is no value for this key (at all, not at any timestamp)
				return GetResult.createNoValueResult(qKey, Period.eternal());
			} else if (higherEntry != null && ceilKey.getKey().equals(key)) {
				// there is no value for this key, until a certain timestamp is reached
				Period period = Period.createRange(0, ceilKey.getTimestamp());
				return GetResult.createNoValueResult(qKey, period);
			}
		} else {
			// we have a "next lower" bound -> we already know that the result will be non-empty.
			// now we need to check if we have an upper bound for the validity of our result...
			if (higherEntry == null || ceilKey.getKey().equals(key) == false) {
				// there is no further value for this key, therefore we have an open-ended period
				Period range = Period.createOpenEndedRange(floorKey.getTimestamp());
				byte[] value = floorEntry.getValue();
				if (value != null && value.length <= 0) {
					// value is non-null, but empty -> it's effectively null
					value = null;
				}
				return GetResult.create(qKey, value, range);
			} else if (higherEntry != null && ceilKey.getKey().equals(key)) {
				// the value of the result is valid between the floor and ceiling entries
				long floorTimestamp = floorKey.getTimestamp();
				long ceilTimestamp = ceilKey.getTimestamp();
				if (floorTimestamp >= ceilTimestamp) {
					ChronoLogger.logError("Invalid 'getRanged' state - floor timestamp (" + floorTimestamp
							+ ") >= ceil timestamp (" + ceilTimestamp + ")! Requested: '" + key + "@" + timestamp
							+ "', floor: '" + floorKey + "', ceil: '" + ceilKey + "'");
				}
				Period period = Period.createRange(floorTimestamp, ceilTimestamp);
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

	public static void putTransactional(final TuplTransaction tx, final String indexName, final String keyspace,
			final long timestamp, final Map<String, byte[]> contents) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(contents, "Precondition violation - argument 'contents' must not be NULL!");
		String inverseIndexName = indexName + INVERSE_MATRIX_SUFFIX;
		for (Entry<String, byte[]> entry : contents.entrySet()) {
			String key = entry.getKey();
			byte[] value = entry.getValue();
			UnqualifiedTemporalKey tk = UnqualifiedTemporalKey.create(key, timestamp);
			if (value != null) {
				logTrace("[PUT] Key = '" + key + "', value = byte[" + value.length + "], timestamp = " + timestamp);
				tx.store(indexName, tk.toSerializableFormat(), value);
			} else {
				logTrace("[PUT] Key = '" + key + "', value = NULL, timestamp = " + timestamp);
				tx.store(indexName, tk.toSerializableFormat(), new byte[0]);
			}
			InverseUnqualifiedTemporalKey itk = InverseUnqualifiedTemporalKey.create(timestamp, key);
			if (value != null) {
				tx.store(inverseIndexName, itk.toSerializableFormat(), TuplUtils.encodeBoolean(true));
			} else {
				tx.store(inverseIndexName, itk.toSerializableFormat(), TuplUtils.encodeBoolean(false));
			}
		}
	}

	public static void putBatch(final TuplTransaction tx, final String indexName, final String keyspace,
			final long timestamp, final Map<String, byte[]> contents) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(contents, "Precondition violation - argument 'contents' must not be NULL!");
		// sort unqualified entries into sorted map
		Map<UnqualifiedTemporalKey, byte[]> data = Maps.newHashMap();
		for (Entry<String, byte[]> entry : contents.entrySet()) {
			String key = entry.getKey();
			byte[] value = entry.getValue();
			UnqualifiedTemporalKey tk = UnqualifiedTemporalKey.create(key, timestamp);
			if (value != null) {
				data.put(tk, value);
			} else {
				data.put(tk, new byte[0]);
			}
		}
		TuplUtils.batchInsertWithoutCheckpoint(tx, indexName, data,
				key -> TuplUtils.encodeString(key.toSerializableFormat()));
		data = null;
		// sort inverse entries (first time, then key)
		String inverseIndexName = indexName + INVERSE_MATRIX_SUFFIX;
		Map<InverseUnqualifiedTemporalKey, byte[]> inverseData = Maps.newHashMap();
		for (Entry<String, byte[]> entry : contents.entrySet()) {
			String key = entry.getKey();
			byte[] value = entry.getValue();
			InverseUnqualifiedTemporalKey itk = InverseUnqualifiedTemporalKey.create(timestamp, key);
			if (value != null) {
				inverseData.put(itk, TuplUtils.encodeBoolean(true));
			} else {
				inverseData.put(itk, TuplUtils.encodeBoolean(false));
			}
		}
		TuplUtils.batchInsertWithoutCheckpoint(tx, inverseIndexName, inverseData,
				key -> TuplUtils.encodeString(key.toSerializableFormat()));
		data = null;
		// persist
		try {
			tx.getDB().checkpoint();
		} catch (IOException e) {
			throw new ChronosIOException("Failed to batch put.", e);
		}
	}

	public static KeySetModifications keySetModifications(final TuplTransaction tx, final String indexName,
			final String keyspace, final long timestamp) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// prepare the database cursor (which navigates in ascending order)
		Cursor cursor = tx.newCursorOn(indexName);
		try {
			// prepare the sets that hold our keyset additions and removals
			Set<String> additions = Sets.newHashSet();
			Set<String> removals = Sets.newHashSet();
			// move the cursor to the very first entry
			cursor.first();
			if (cursor.key() == null) {
				// the keyspace is empty
				return new KeySetModifications(additions, removals);
			}
			// iterate over the full B-Tree key set (ascending order)
			while (cursor.key() != null) {
				String key = TuplUtils.decodeString(cursor.key());
				byte[] value = cursor.value();
				UnqualifiedTemporalKey currentKey = UnqualifiedTemporalKey.parseSerializableFormat(key);
				if (currentKey.getTimestamp() > timestamp) {
					cursor.next();
					continue;
				}
				String plainKey = currentKey.getKey();
				if (value == null || value.length <= 0) {
					// removal
					additions.remove(plainKey);
					removals.add(plainKey);
				} else {
					// put
					additions.add(plainKey);
					removals.remove(plainKey);
				}
				// move to the next entry
				cursor.next();
			}
			return new KeySetModifications(additions, removals);
		} catch (IOException ioe) {
			throw new ChronosIOException("Failed to load keyset modifications! See root cause for details.", ioe);
		} finally {
			if (cursor != null) {
				cursor.reset();
			}
		}
	}

	public static Iterator<Long> history(final TuplTransaction tx, final String indexName, final String keyspace,
			final long maxTime, final String key) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(maxTime >= 0, "Precondition violation - argument 'maxTime' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		logTrace(
				"[HST] Retrieving history of key '" + key + "' in keyspace '" + keyspace + "' at timestamp " + maxTime);
		String tkMin = UnqualifiedTemporalKey.createMin(key).toSerializableFormat();
		String tkMax = UnqualifiedTemporalKey.create(key, maxTime).toSerializableFormat();
		byte[] tkMinEnc = TuplUtils.encodeString(tkMin);
		byte[] tkMaxEnc = TuplUtils.encodeString(tkMax);
		// prepare the list of timestamps (this is eager, lazy processing here is not really feasible)
		List<Long> timestampsDescending = Lists.newArrayList();
		Cursor cursor = tx.newCursorOn(indexName);
		try {
			// disable auto-loading of values for this cursor; we are interested exclusively in keys here.
			cursor.autoload(false);
			// we start with our cursor at the upper bound (inclusive) and traverse downwards
			cursor.findLe(tkMaxEnc);
			if (cursor.key() == null) {
				// keyspace is empty
				return Collections.emptyIterator();
			}
			while (cursor.key() != null && cursor.compareKeyTo(tkMinEnc) >= 0) {
				String entryKey = TuplUtils.decodeString(cursor.key());
				UnqualifiedTemporalKey tKey = UnqualifiedTemporalKey.parseSerializableFormat(entryKey);
				timestampsDescending.add(tKey.getTimestamp());
				// move the cursor
				cursor.previous();
			}
		} catch (IOException ioe) {
			throw new ChronosIOException("Failed to load key history! See root cause for details.", ioe);
		} finally {
			if (cursor != null) {
				cursor.reset();
			}
		}
		return timestampsDescending.iterator();
	}

	public static void insertEntriesTransactional(final TuplTransaction tx, final String indexName,
			final String keyspace, final Set<UnqualifiedTemporalEntry> entries) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
		logTrace("[INS] Transaction-based inserting " + entries.size() + " entries into keyspace '" + keyspace + "'.");
		if (entries.isEmpty()) {
			// there is nothing to insert...
			return;
		}
		for (UnqualifiedTemporalEntry entry : entries) {
			UnqualifiedTemporalKey key = entry.getKey();
			byte[] value = entry.getValue();
			tx.store(indexName, key.toSerializableFormat(), value);
		}
	}

	public static void insertEntriesBatch(final TuplTransaction tx, final String indexName, final String keyspace,
			final Set<UnqualifiedTemporalEntry> entries) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
		logTrace("[INS] Batch-based inserting " + entries.size() + " entries into keyspace '" + keyspace + "'.");
		if (entries.isEmpty()) {
			// there is nothing to insert...
			return;
		}
		// sort entries
		Map<UnqualifiedTemporalKey, byte[]> data = Maps.newHashMap();
		for (UnqualifiedTemporalEntry entry : entries) {
			data.put(entry.getKey(), entry.getValue());
		}
		// insert entries
		TuplUtils.batchInsertWithoutCheckpoint(tx, indexName, data,
				key -> TuplUtils.encodeString(key.toSerializableFormat()));
		try {
			tx.getDB().checkpoint();
		} catch (IOException e) {
			throw new ChronosIOException("Failed to batch insert. See root cause for details.", e);
		}
	}

	public static long lastCommitTimestamp(final TuplTransaction tx, final String indexName, final String keyspace,
			final String key) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		logTrace("[LCT] Retrieving last commit timestamp in keyspace '" + keyspace + "' on key '" + key + "'");
		String lastKey = floorKey(tx, indexName, key, Long.MAX_VALUE);
		if (lastKey == null) {
			return -1;
		}
		UnqualifiedTemporalKey lastKeyParsed = UnqualifiedTemporalKey.parseSerializableFormat(lastKey);
		if (lastKeyParsed.getKey().equals(key) == false) {
			return -1;
		}
		return lastKeyParsed.getTimestamp();
	}

	public static void rollback(final TuplTransaction tx, final String indexName, final long timestamp) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// in this case, we have to do a linear scan of both the key-time and the time-key index, remember
		// the keys to delete, and then delete them afterwards. In Tuple, there is no "delete as you go" mode
		// of operation for cursors, i.e. the cursor equivalent of "iterator.remove()" is missing.

		// TODO PERFORMANCE TUPL: Potential optimization opportunity: Index#evict(...)

		{ // update the key-time index
			Cursor cursor = tx.newCursorOn(indexName);
			List<byte[]> keysToRemove = Lists.newArrayList();
			try {
				// we are not interested in the values; disable value auto-load for this cursor
				cursor.autoload(false);
				// move the cursor to the first entry
				cursor.first();
				while (cursor.key() != null) {
					byte[] tuplKey = cursor.key();
					String tKeyString = TuplUtils.decodeString(tuplKey);
					UnqualifiedTemporalKey tKey = UnqualifiedTemporalKey.parseSerializableFormat(tKeyString);
					if (tKey.getTimestamp() > timestamp) {
						keysToRemove.add(tuplKey);
					}
					cursor.next();
				}
			} catch (IOException ioe) {
				throw new ChronosIOException("Failed to roll back primary index! See root cause for details.", ioe);
			} finally {
				if (cursor != null) {
					cursor.reset();
				}
			}
			// remove the keys we need to delete
			for (byte[] key : keysToRemove) {
				tx.delete(indexName, key);
			}
		}

		{ // update the time-key index
			String inverseIndexName = indexName + INVERSE_MATRIX_SUFFIX;
			Cursor cursor = tx.newCursorOn(inverseIndexName);
			List<byte[]> keysToRemove = Lists.newArrayList();
			try {
				// we are not interested in the values; disable value auto-load for this cursor
				cursor.autoload(false);
				// move the cursor to the first entry
				cursor.first();
				while (cursor.key() != null) {
					byte[] tuplKey = cursor.key();
					String tKeyString = TuplUtils.decodeString(tuplKey);
					InverseUnqualifiedTemporalKey tKey = InverseUnqualifiedTemporalKey
							.parseSerializableFormat(tKeyString);
					if (tKey.getTimestamp() > timestamp) {
						keysToRemove.add(tuplKey);
					}
					cursor.next();
				}
			} catch (IOException ioe) {
				throw new ChronosIOException("Failed to roll back primary index! See root cause for details.", ioe);
			} finally {
				if (cursor != null) {
					cursor.reset();
				}
			}
			// remove the keys we need to delete
			for (byte[] key : keysToRemove) {
				tx.delete(indexName, key);
			}
		}
	}

	public static Iterator<TemporalKey> getModificationsBetween(final TuplTransaction tx, final String indexName,
			final String keyspace, final long timestampLowerBound, final long timestampUpperBound) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		InverseUnqualifiedTemporalKey itkLow = InverseUnqualifiedTemporalKey.createMinInclusive(timestampLowerBound);
		InverseUnqualifiedTemporalKey itkHigh = InverseUnqualifiedTemporalKey.createMaxExclusive(timestampUpperBound);
		byte[] itkLowEnc = TuplUtils.encodeString(itkLow.toSerializableFormat());
		byte[] itkHighEnc = TuplUtils.encodeString(itkHigh.toSerializableFormat());
		// prepare the list of timestamps (this is eager, lazy processing here is not really feasible)
		List<TemporalKey> tKeysDescending = Lists.newArrayList();
		Cursor cursor = tx.newCursorOn(indexName + INVERSE_MATRIX_SUFFIX);
		try {
			// disable auto-loading of values for this cursor; we are interested exclusively in keys here.
			cursor.autoload(false);
			// we start with our cursor at the upper bound (exclusive) and traverse downwards
			cursor.findLt(itkHighEnc);
			if (cursor.key() == null) {
				// keyspace is empty
				return Collections.emptyIterator();
			}
			while (cursor.key() != null && cursor.compareKeyTo(itkLowEnc) >= 0) {
				String entryKey = TuplUtils.decodeString(cursor.key());
				InverseUnqualifiedTemporalKey tKey = InverseUnqualifiedTemporalKey.parseSerializableFormat(entryKey);
				tKeysDescending.add(TemporalKey.create(tKey.getTimestamp(), keyspace, tKey.getKey()));
				// move the cursor
				cursor.previous();
			}
		} catch (IOException ioe) {
			throw new ChronosIOException("Failed to load key history! See root cause for details.", ioe);
		} finally {
			if (cursor != null) {
				cursor.reset();
			}
		}
		return tKeysDescending.iterator();
	}

	public static CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final TuplTransaction tx,
			final String indexName, final long maxTimestamp) {
		return new AllEntriesIterator(tx, indexName, maxTimestamp);
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	protected static Pair<Entry<String, byte[]>, Entry<String, byte[]>> floorEntryAndHigherEntry(
			final TuplTransaction tx, final String indexName, final String key, final long timestamp) {
		byte[] searchKey = TuplUtils.encodeString(UnqualifiedTemporalKey.create(key, timestamp).toSerializableFormat());
		Cursor cursor = tx.newCursorOn(indexName);
		Entry<String, byte[]> floorEntry = null;
		Entry<String, byte[]> higherEntry = null;
		try {
			cursor.autoload(true);
			cursor.findLe(searchKey);
			byte[] binaryFloorValue = cursor.value();
			byte[] binaryFloorKey = cursor.key();
			if (binaryFloorKey == null || binaryFloorKey.length <= 0) {
				// there is no floor entry for the requested key; check the higher entry explicitly
				floorEntry = null;
				cursor.findGt(searchKey);
				byte[] binaryHigherKey = cursor.key();
				byte[] binaryHigherValue = cursor.value();
				if (binaryHigherKey == null || binaryHigherKey.length <= 0) {
					// there is no higher entry
					higherEntry = null;
				} else {
					// there is no floor entry, but a higher entry
					higherEntry = Pair.of(TuplUtils.decodeString(binaryHigherKey), binaryHigherValue);
				}
			} else {
				// found a floor entry
				floorEntry = Pair.of(TuplUtils.decodeString(binaryFloorKey), binaryFloorValue);
				// move next() until a higher entry is found
				do {
					cursor.next();
				} while (cursor.key() != null && cursor.compareKeyTo(searchKey) <= 0);
				byte[] binaryHigherKey = cursor.key();
				byte[] binaryHigherValue = cursor.value();
				if (binaryHigherKey == null || binaryHigherKey.length <= 0) {
					// there is no higher entry
					higherEntry = null;
				} else {
					// there is no floor entry, but a higher entry
					higherEntry = Pair.of(TuplUtils.decodeString(binaryHigherKey), binaryHigherValue);
				}
			}
			return Pair.of(floorEntry, higherEntry);
		} catch (IOException ioe) {
			throw new ChronosIOException("Failed to query matrix contents. See root cause for details.", ioe);
		} finally {
			if (cursor != null) {
				cursor.reset();
			}
		}
	}

	protected static String lowerKey(final TuplTransaction tx, final String indexName, final String key,
			final long timestamp) {
		return findExistingKey(tx, indexName, key, timestamp, SearchMode.LESS_THAN);
	}

	protected static String higherKey(final TuplTransaction tx, final String indexName, final String key,
			final long timestamp) {
		return findExistingKey(tx, indexName, key, timestamp, SearchMode.GREATER_THAN);
	}

	protected static String floorKey(final TuplTransaction tx, final String indexName, final String key,
			final long timestamp) {
		return findExistingKey(tx, indexName, key, timestamp, SearchMode.LESS_EQUAL);
	}

	protected static String ceilKey(final TuplTransaction tx, final String indexName, final String key,
			final long timestamp) {
		return findExistingKey(tx, indexName, key, timestamp, SearchMode.GREATER_EQUAL);
	}

	protected static Entry<String, byte[]> lowerEntry(final TuplTransaction tx, final String indexName,
			final String key, final long timestamp) {
		return findEntry(tx, indexName, key, timestamp, SearchMode.LESS_THAN, true);
	}

	protected static Entry<String, byte[]> higherEntry(final TuplTransaction tx, final String indexName,
			final String key, final long timestamp) {
		return findEntry(tx, indexName, key, timestamp, SearchMode.GREATER_THAN, true);
	}

	protected static Entry<String, byte[]> floorEntry(final TuplTransaction tx, final String indexName,
			final String key, final long timestamp) {
		return findEntry(tx, indexName, key, timestamp, SearchMode.LESS_EQUAL, true);
	}

	protected static Entry<String, byte[]> ceilEntry(final TuplTransaction tx, final String indexName, final String key,
			final long timestamp) {
		return findEntry(tx, indexName, key, timestamp, SearchMode.GREATER_EQUAL, true);
	}

	protected static String findExistingKey(final TuplTransaction tx, final String indexName, final String key,
			final long timestamp, final SearchMode mode) {
		Entry<String, byte[]> entry = findEntry(tx, indexName, key, timestamp, mode, false);
		if (entry == null) {
			return null;
		} else {
			return entry.getKey();
		}
	}

	private static Entry<String, byte[]> findEntry(final TuplTransaction tx, final String indexName, final String key,
			final long timestamp, final SearchMode mode, final boolean loadValue) {
		byte[] searchKey = TuplUtils.encodeString(UnqualifiedTemporalKey.create(key, timestamp).toSerializableFormat());
		Cursor cursor = tx.newCursorOn(indexName);
		try {
			cursor.autoload(loadValue);
			switch (mode) {
			case GREATER_THAN:
				cursor.findGt(searchKey);
				break;
			case GREATER_EQUAL:
				cursor.findGe(searchKey);
				break;
			case LESS_THAN:
				cursor.findLt(searchKey);
				break;
			case LESS_EQUAL:
				cursor.findLe(searchKey);
				break;
			default:
				throw new UnknownEnumLiteralException(mode);
			}
			byte[] binaryValue = null;
			if (loadValue) {
				binaryValue = cursor.value();
			}
			byte[] binaryKey = cursor.key();
			if (binaryKey == null || binaryKey.length <= 0) {
				// there is no entry for the requested key
				return null;
			}
			String resultKey = TuplUtils.decodeString(binaryKey);
			return Pair.of(resultKey, binaryValue);
		} catch (IOException ioe) {
			throw new ChronosIOException("Failed to query matrix contents. See root cause for details.", ioe);
		} finally {
			if (cursor != null) {
				cursor.reset();
			}
		}
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static enum SearchMode {

		GREATER_EQUAL, GREATER_THAN, LESS_THAN, LESS_EQUAL

	}

	private static class AllEntriesIterator extends AbstractCloseableIterator<UnqualifiedTemporalEntry> {

		private final TuplTransaction tx;
		private final long maxTimestamp;
		private final Cursor cursor;

		private AllEntriesIterator(final TuplTransaction tx, final String indexName, final long maxTimestamp) {
			checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
			checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
			checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
			this.tx = tx;
			this.maxTimestamp = maxTimestamp;
			this.cursor = tx.newCursorOn(indexName);
			try {
				this.cursor.autoload(false);
				// start at the first entry
				this.cursor.first();
				// check if the first entry has already a timestamp larger than the maximum
				// (note: this index is FIRST ordered by user key and THEN by timestamp)
				byte[] key = this.cursor.key();
				// note: we need to perform this check if the key is NULL here in case the matrix is empty.
				if (key != null) {
					String keyString = TuplUtils.decodeString(key);
					UnqualifiedTemporalKey tKey = UnqualifiedTemporalKey.parseSerializableFormat(keyString);
					if (tKey.getTimestamp() > maxTimestamp) {
						// timestamp limit exceeded; find the next matching entry
						this.advanceCursor();
					}
				}
			} catch (IOException e) {
				throw new ChronosIOException("Failed to stream entries! See root cause for details.", e);
			}
		}

		@Override
		public UnqualifiedTemporalEntry next() {
			if (this.hasNext() == false) {
				throw new NoSuchElementException("Iterator is exhausted; there are no more elements!");
			}
			byte[] key = this.cursor.key();
			try {
				// explicitly load the value at this cursor position (autoload is disabled here)
				this.cursor.load();
			} catch (IOException ioe) {
				throw new ChronosIOException("Failed to stream entries! See root cause for details.", ioe);
			}
			byte[] value = this.cursor.value();
			this.advanceCursor();
			UnqualifiedTemporalKey tKey = UnqualifiedTemporalKey.parseSerializableFormat(TuplUtils.decodeString(key));
			return new UnqualifiedTemporalEntry(tKey, value);
		}

		@Override
		protected boolean hasNextInternal() {
			return this.cursor.key() != null;
		}

		@Override
		protected void closeInternal() {
			this.cursor.reset();
			this.tx.close();
		}

		private void advanceCursor() {
			try {
				while (this.cursor.key() != null) {
					this.cursor.next();
					byte[] key = this.cursor.key();
					if (key == null) {
						// end of iteration
						return;
					}
					// deserialize the key to check the timestamp on it
					String keyString = TuplUtils.decodeString(key);
					UnqualifiedTemporalKey tKey = UnqualifiedTemporalKey.parseSerializableFormat(keyString);
					if (tKey.getTimestamp() <= this.maxTimestamp) {
						// found a matching key
						return;
					}
				}
			} catch (IOException ioe) {
				throw new ChronosIOException("Failed to stream entries! See root cause for details.", ioe);
			}
		}
	}

}
