package org.chronos.chronodb.internal.impl.engines.tupl;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalDataMatrix;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.util.KeySetModifications;

public class TemporalTuplMatrix extends AbstractTemporalDataMatrix {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final TuplChronoDB db;
	private final String indexName;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public TemporalTuplMatrix(final String keyspace, final long timestamp, final TuplChronoDB db,
			final String indexName) {
		super(keyspace, timestamp);
		checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.db = db;
		this.indexName = indexName;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public GetResult<byte[]> get(final long timestamp, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		try (DefaultTuplTransaction tx = this.db.openTransaction()) {
			return TuplDataMatrixUtil.get(tx, this.indexName, this.getKeyspace(), timestamp, key);
		}
	}

	@Override
	public void put(final long time, final Map<String, byte[]> contents) {
		if (contents.size() > TuplUtils.BATCH_INSERT_THRESHOLD) {
			// perform batch insert
			try (DefaultTuplTransaction tx = this.db.openBogusTransaction()) {
				TuplDataMatrixUtil.putBatch(tx, this.indexName, this.getKeyspace(), time, contents);
				tx.commit();
			}
		} else {
			// perform transactional insert
			try (DefaultTuplTransaction tx = this.db.openTransaction()) {
				TuplDataMatrixUtil.putTransactional(tx, this.indexName, this.getKeyspace(), time, contents);
				tx.commit();
			}
		}
	}

	@Override
	public KeySetModifications keySetModifications(final long timestamp) {
		try (DefaultTuplTransaction tx = this.db.openTransaction()) {
			return TuplDataMatrixUtil.keySetModifications(tx, this.indexName, this.getKeyspace(), timestamp);
		}
	}

	@Override
	public Iterator<Long> history(final long maxTime, final String key) {
		try (DefaultTuplTransaction tx = this.db.openTransaction()) {
			return TuplDataMatrixUtil.history(tx, this.indexName, this.getKeyspace(), maxTime, key);
		}
	}

	@Override
	public void insertEntries(final Set<UnqualifiedTemporalEntry> entries) {
		if (entries.size() > TuplUtils.BATCH_INSERT_THRESHOLD) {
			// perform batch insert
			try (DefaultTuplTransaction tx = this.db.openBogusTransaction()) {
				TuplDataMatrixUtil.insertEntriesBatch(tx, this.indexName, this.getKeyspace(), entries);
				tx.commit();
			}
		} else {
			// perform transactional insert
			try (DefaultTuplTransaction tx = this.db.openTransaction()) {
				TuplDataMatrixUtil.insertEntriesTransactional(tx, this.indexName, this.getKeyspace(), entries);
				tx.commit();
			}
		}
	}

	@Override
	public CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final long maxTimestamp) {
		logTrace("[AEN] Retrieving all entries in keyspace '" + this.getKeyspace() + "' before " + maxTimestamp);
		// this transaction is intentionally left open; it will be closed by the closeable iterator returned by this
		// method.
		DefaultTuplTransaction tx = this.db.openBogusTransaction();
		return TuplDataMatrixUtil.allEntriesIterator(tx, this.indexName, maxTimestamp);
	}

	@Override
	public long lastCommitTimestamp(final String key) {
		try (DefaultTuplTransaction tx = this.db.openTransaction()) {
			return TuplDataMatrixUtil.lastCommitTimestamp(tx, this.indexName, this.getKeyspace(), key);
		}
	}

	@Override
	public void rollback(final long timestamp) {
		try (DefaultTuplTransaction tx = this.db.openTransaction()) {
			TuplDataMatrixUtil.rollback(tx, this.indexName, timestamp);
			tx.commit();
		}
	}

	@Override
	public Iterator<TemporalKey> getModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		try (DefaultTuplTransaction tx = this.db.openTransaction()) {
			return TuplDataMatrixUtil.getModificationsBetween(tx, this.indexName, this.getKeyspace(),
					timestampLowerBound, timestampUpperBound);
		}
	}

	public String getIndexName() {
		return this.indexName;
	}

}
