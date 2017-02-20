package org.chronos.chronodb.internal.impl.engines.mapdb;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalDataMatrix;
import org.chronos.chronodb.internal.impl.mapdb.MapDBDataMatrixUtil;
import org.chronos.chronodb.internal.impl.mapdb.MapDBTransaction;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.util.KeySetModifications;
import org.mapdb.Serializer;

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
	public GetResult<byte[]> get(final long timestamp, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		try (MapDBTransaction tx = this.openTransaction()) {
			return MapDBDataMatrixUtil.get(tx, this.mapName, this.getKeyspace(), timestamp, key);
		}
	}

	@Override
	public void put(final long time, final Map<String, byte[]> contents) {
		try (MapDBTransaction tx = this.openTransaction()) {
			MapDBDataMatrixUtil.put(tx, this.mapName, this.getKeyspace(), time, contents);
			tx.commit();
		}
	}

	@Override
	public KeySetModifications keySetModifications(final long timestamp) {
		try (MapDBTransaction tx = this.openTransaction()) {
			return MapDBDataMatrixUtil.keySetModifications(tx, this.mapName, this.getKeyspace(), timestamp);
		}
	}

	@Override
	public Iterator<Long> history(final long maxTime, final String key) {
		try (MapDBTransaction tx = this.openTransaction()) {
			return MapDBDataMatrixUtil.history(tx, this.mapName, this.getKeyspace(), maxTime, key);
		}
	}

	@Override
	public void insertEntries(final Set<UnqualifiedTemporalEntry> entries) {
		try (MapDBTransaction tx = this.openTransaction()) {
			MapDBDataMatrixUtil.insertEntries(tx, this.mapName, this.getKeyspace(), entries);
			tx.commit();
		}
	}

	@Override
	public CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final long maxTimestamp) {
		logTrace("[AEN] Retrieving all entries in keyspace '" + this.getKeyspace() + "' before " + maxTimestamp);
		MapDBTransaction tx = this.openTransaction();
		// tx remains open; the iterator.close() method closes the transaction.
		return MapDBDataMatrixUtil.allEntriesIterator(tx, this.getMapName(), this.getKeyspace(), maxTimestamp);
	}

	@Override
	public long lastCommitTimestamp(final String key) {
		try (MapDBTransaction tx = this.openTransaction()) {
			return MapDBDataMatrixUtil.lastCommitTimestamp(tx, this.mapName, this.getKeyspace(), key);
		}
	}

	@Override
	public void rollback(final long timestamp) {
		try (MapDBTransaction tx = this.openTransaction()) {
			MapDBDataMatrixUtil.rollback(tx, this.mapName, timestamp);
			tx.commit();
		}
	}

	@Override
	public Iterator<TemporalKey> getModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		try (MapDBTransaction tx = this.openTransaction()) {
			return MapDBDataMatrixUtil.getModificationsBetween(tx, this.mapName, this.getKeyspace(),
					timestampLowerBound, timestampUpperBound);
		}
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

}
