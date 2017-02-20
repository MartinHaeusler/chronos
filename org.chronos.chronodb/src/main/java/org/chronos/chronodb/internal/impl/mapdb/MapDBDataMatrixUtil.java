package org.chronos.chronodb.internal.impl.mapdb;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;

import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.chronodb.internal.util.DataMatrixUtil;
import org.chronos.chronodb.internal.util.KeySetModifications;
import org.mapdb.Serializer;

import com.google.common.collect.Iterators;

public class MapDBDataMatrixUtil {

	private static final String INVERSE_MATRIX_SUFFIX = "_inv";

	public static GetResult<byte[]> get(final MapDBTransaction tx, final String mapName, final String keyspace,
			final long timestamp, final String key) {
		return DataMatrixUtil.get(getMapReadOnly(tx, mapName), keyspace, timestamp, key);
	}

	public static void put(final MapDBTransaction tx, final String mapName, final String keyspace, final long time,
			final Map<String, byte[]> contents) {
		DataMatrixUtil.put(getMapReadWrite(tx, mapName), getMapInverseReadWrite(tx, mapName), keyspace, time, contents);
	}

	public static KeySetModifications keySetModifications(final MapDBTransaction tx, final String mapName,
			final String keyspace, final long timestamp) {
		return DataMatrixUtil.keySetModifications(getMapReadOnly(tx, mapName), keyspace, timestamp);
	}

	public static Iterator<Long> history(final MapDBTransaction tx, final String mapName, final String keyspace,
			final long maxTime, final String key) {
		return DataMatrixUtil.history(getMapReadOnly(tx, mapName), keyspace, maxTime, key);
	}

	public static void insertEntries(final MapDBTransaction tx, final String mapName, final String keyspace,
			final Set<UnqualifiedTemporalEntry> entries) {
		DataMatrixUtil.insertEntries(getMapReadWrite(tx, mapName), keyspace, entries);
	}

	public static CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final MapDBTransaction tx,
			final String mapName, final String keyspace, final long maxTimestamp) {
		logTrace("[AEN] Retrieving all entries in keyspace '" + keyspace + "' before " + maxTimestamp);
		return new AllEntriesIterator(tx, getMapReadOnly(tx, mapName), maxTimestamp);
	}

	public static long lastCommitTimestamp(final MapDBTransaction tx, final String mapName, final String keyspace,
			final String key) {
		return DataMatrixUtil.lastCommitTimestamp(getMapReadOnly(tx, mapName), keyspace, key);
	}

	public static void rollback(final MapDBTransaction tx, final String mapName, final long timestamp) {
		DataMatrixUtil.rollback(getMapReadWrite(tx, mapName), getMapInverseReadWrite(tx, mapName), timestamp);
	}

	public static Iterator<TemporalKey> getModificationsBetween(final MapDBTransaction tx, final String mapName,
			final String keyspace, final long timestampLowerBound, final long timestampUpperBound) {
		return DataMatrixUtil.getModificationsBetween(getMapInverseReadOnly(tx, mapName), keyspace, timestampLowerBound,
				timestampUpperBound);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	protected static NavigableMap<String, byte[]> getMapReadWrite(final MapDBTransaction tx, final String mapName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(mapName, "Precondition violation - argument 'mapName' must not be NULL!");
		return tx.treeMap(mapName, Serializer.STRING, Serializer.BYTE_ARRAY);
	}

	protected static NavigableMap<String, byte[]> getMapReadOnly(final MapDBTransaction tx, final String mapName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(mapName, "Precondition violation - argument 'mapName' must not be NULL!");
		if (tx.exists(mapName) == false) {
			return Collections.emptyNavigableMap();
		} else {
			return tx.treeMap(mapName, Serializer.STRING, Serializer.BYTE_ARRAY);
		}

	}

	protected static NavigableMap<String, Boolean> getMapInverseReadWrite(final MapDBTransaction tx,
			final String mapName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(mapName, "Precondition violation - argument 'mapName' must not be NULL!");
		return tx.treeMap(mapName + INVERSE_MATRIX_SUFFIX, Serializer.STRING, Serializer.BOOLEAN);
	}

	protected static NavigableMap<String, Boolean> getMapInverseReadOnly(final MapDBTransaction tx,
			final String mapName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(mapName, "Precondition violation - argument 'mapName' must not be NULL!");
		String inverseMapName = mapName + INVERSE_MATRIX_SUFFIX;
		if (tx.exists(inverseMapName) == false) {
			return Collections.emptyNavigableMap();
		} else {
			return tx.treeMap(inverseMapName, Serializer.STRING, Serializer.BOOLEAN);
		}
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private static class AllEntriesIterator extends AbstractCloseableIterator<UnqualifiedTemporalEntry> {

		private final MapDBTransaction tx;
		private final Iterator<Entry<String, byte[]>> entryIterator;

		public AllEntriesIterator(final MapDBTransaction tx, final NavigableMap<String, byte[]> map,
				final long maxTimestamp) {
			checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
			checkNotNull(map, "Precondition violation - argument 'map' must not be NULL!");
			checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
			this.tx = tx;
			// iterate all keys with a timestamp up to the given one
			this.entryIterator = Iterators.filter(map.entrySet().iterator(), entry -> UnqualifiedTemporalKey
					.parseSerializableFormat(entry.getKey()).getTimestamp() <= maxTimestamp);
		}

		@Override
		protected boolean hasNextInternal() {
			return this.entryIterator.hasNext();
		}

		@Override
		public UnqualifiedTemporalEntry next() {
			if (this.hasNext() == false) {
				throw new NoSuchElementException("Iterator is exhausted, there are no more elements.");
			}
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
