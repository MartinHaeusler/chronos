package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalDataMatrix;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplDataMatrixUtil;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplUtils;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.chronodb.internal.util.KeySetModifications;

import com.google.common.collect.Lists;

public class TemporalChunkDbDataMatrix extends AbstractTemporalDataMatrix {

	private final GlobalChunkManager chunkManager;
	private final String branchName;

	private final String mapName;

	protected TemporalChunkDbDataMatrix(final GlobalChunkManager chunkManager, final String branchName,
			final String mapName, final String keyspace, final long creationTimestamp) {
		super(keyspace, creationTimestamp);
		checkNotNull(chunkManager, "Precondition violation - argument 'chunkManager' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(mapName, "Precondition violation - argument 'mapName' must not be NULL!");
		this.chunkManager = chunkManager;
		this.branchName = branchName;
		this.mapName = mapName;
	}

	@Override
	public GetResult<byte[]> get(final long timestamp, final String key) {
		try (ChunkTuplTransaction tx = this.chunkManager.openTransactionOn(this.branchName, timestamp)) {
			GetResult<byte[]> getResult = TuplDataMatrixUtil.get(tx, this.mapName, this.getKeyspace(), timestamp, key);
			Period chunkPeriod = tx.getChunkPeriod();
			if (getResult.getPeriod().getUpperBound() > chunkPeriod.getUpperBound()) {
				// the get result states a higher "valid to" than the chunk itself; we need
				// to limit the "valid to" timestamp to the chunk's "valid to"
				Period newPeriod = Period.createRange(getResult.getPeriod().getLowerBound(),
						chunkPeriod.getUpperBound());
				getResult = GetResult.alterPeriod(getResult, newPeriod);
			}
			return getResult;
		}
	}

	@Override
	public KeySetModifications keySetModifications(final long timestamp) {
		try (TuplTransaction tx = this.chunkManager.openTransactionOn(this.branchName, timestamp)) {
			return TuplDataMatrixUtil.keySetModifications(tx, this.mapName, this.getKeyspace(), timestamp);
		}
	}

	@Override
	public Iterator<Long> history(final long maxTime, final String key) {
		long upperBound = maxTime;
		if (upperBound < Long.MAX_VALUE) {
			// note: for this method, the "maxTime" is an inclusive value, which is why we add 1 here
			upperBound += 1;
		}
		BranchChunkManager branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName);
		List<ChronoChunk> chunksForPeriod = branchChunkManager.getChunksForPeriod(Period.createRange(0, upperBound));
		// descending order of chunk files
		chunksForPeriod = Lists.reverse(chunksForPeriod);
		return new HistoryIterator(chunksForPeriod, maxTime, key);
	}

	@Override
	public CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final long timestamp) {
		long upperBound = timestamp;
		if (upperBound < Long.MAX_VALUE) {
			// note: for this method, the "timestamp" is an inclusive value, which is why we add 1 here
			upperBound += 1;
		}
		BranchChunkManager branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName);
		List<ChronoChunk> chunksForPeriod = branchChunkManager.getChunksForPeriod(Period.createRange(0, upperBound));
		return new AllEntriesIterator(chunksForPeriod, timestamp);
	}

	@Override
	public long lastCommitTimestamp(final String key) {
		Iterator<Long> history = this.history(Long.MAX_VALUE, key);
		if (history.hasNext()) {
			return history.next();
		} else {
			return -1;
		}
	}

	@Override
	public Iterator<TemporalKey> getModificationsBetween(final long timestampLowerBound,
			final long timestampUpperBound) {
		long upperBound = timestampUpperBound;
		if (upperBound < Long.MAX_VALUE) {
			// note: for this operation, the upper bound is inclusive, which is why we add 1
			upperBound += 1;
		}
		Period period = Period.createRange(timestampLowerBound, upperBound);
		BranchChunkManager branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName);
		List<ChronoChunk> chunksForPeriod = branchChunkManager.getChunksForPeriod(period);
		// descending order of chunk files
		chunksForPeriod = Lists.reverse(chunksForPeriod);
		return new ModificationsIterator(chunksForPeriod, period);
	}

	@Override
	public void put(final long timestamp, final Map<String, byte[]> contents) {
		BranchChunkManager branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName);
		ChronoChunk chunk = branchChunkManager.getChunkForHeadRevision();
		if (contents.size() > TuplUtils.BATCH_INSERT_THRESHOLD) {
			// perform batch put
			try (TuplTransaction tx = this.chunkManager.openBogusTransactionOn(chunk.getDataFile())) {
				TuplDataMatrixUtil.putBatch(tx, this.mapName, this.getKeyspace(), timestamp, contents);
				tx.commit();
			}
		} else {
			// perform transactional put
			try (TuplTransaction tx = this.chunkManager.openTransactionOn(chunk.getDataFile())) {
				TuplDataMatrixUtil.putTransactional(tx, this.mapName, this.getKeyspace(), timestamp, contents);
				tx.commit();
			}
		}
	}

	@Override
	public void insertEntries(final Set<UnqualifiedTemporalEntry> entries) {
		BranchChunkManager branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName);
		ChronoChunk chunk = branchChunkManager.getChunkForHeadRevision();
		Period chunkPeriod = chunk.getMetaData().getValidPeriod();
		// check if all entries are within head revision bounds
		for (UnqualifiedTemporalEntry entry : entries) {
			if (chunkPeriod.contains(entry.getKey().getTimestamp()) == false) {
				throw new IllegalStateException(
						"Entry at '" + entry.getKey() + "' is out of bounds of head revision chunk");
			}
		}
		if (entries.size() > TuplUtils.BATCH_INSERT_THRESHOLD) {
			// perform batch insert
			try (TuplTransaction tx = this.chunkManager.openBogusTransactionOn(chunk.getDataFile())) {
				TuplDataMatrixUtil.insertEntriesBatch(tx, this.mapName, this.getKeyspace(), entries);
				tx.commit();
			}
		} else {
			// perform transactional insert
			try (TuplTransaction tx = this.chunkManager.openTransactionOn(chunk.getDataFile())) {
				TuplDataMatrixUtil.insertEntriesTransactional(tx, this.mapName, this.getKeyspace(), entries);
				tx.commit();
			}
		}
	}

	@Override
	public void rollback(final long timestamp) {
		BranchChunkManager branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName);
		ChronoChunk chunk = branchChunkManager.getChunkForTimestamp(timestamp);
		Period chunkFilePeriod = chunk.getMetaData().getValidPeriod();
		if (chunkFilePeriod.isOpenEnded() == false) {
			throw new IllegalStateException("Cannot roll back! Timestamp '" + timestamp + "' is not in head revision.");
		}
		if (chunkFilePeriod.contains(timestamp) == false || chunkFilePeriod.getLowerBound() > timestamp) {
			throw new IllegalStateException("Timestamp '" + timestamp + "' not within chunk!");
		}
		try (TuplTransaction tx = this.chunkManager.openTransactionOn(this.branchName, timestamp)) {
			TuplDataMatrixUtil.rollback(tx, this.mapName, timestamp);
			tx.commit();
		}
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private abstract class LazyChunkIterator<T> implements Iterator<T> {

		private final Iterator<ChronoChunk> chunkIterator;
		private Iterator<T> currentChunkElementIterator = null;

		public LazyChunkIterator(final Iterator<ChronoChunk> chunkIterator) {
			checkNotNull(chunkIterator, "Precondition violation - argument 'chunkIterator' must not be NULL!");
			this.chunkIterator = chunkIterator;
		}

		protected void moveToNextChunkIfExhausted() {
			if (this.hasNext()) {
				// current chunk is not exhausted yet
				return;
			}
			if (this.chunkIterator.hasNext() == false) {
				// no new chunk to move to
				this.currentChunkElementIterator = null;
				return;
			}
			// fetch chunk
			this.currentChunkElementIterator = null;
			while (this.currentChunkElementIterator == null && this.chunkIterator.hasNext()) {
				ChronoChunk chunk = this.chunkIterator.next();
				this.currentChunkElementIterator = this.createChunkElementIterator(chunk);
				if (this.currentChunkElementIterator != null && this.currentChunkElementIterator.hasNext() == false) {
					this.currentChunkElementIterator = null;
				}
			}
		}

		@Override
		public boolean hasNext() {
			if (this.currentChunkElementIterator != null && this.currentChunkElementIterator.hasNext()) {
				// still some entries left in current chunk
				return true;
			} else {
				return false;
			}
		}

		@Override
		public T next() {
			if (this.hasNext() == false) {
				throw new IllegalStateException("Iterator has no more elements!");
			}
			T element = this.currentChunkElementIterator.next();
			this.moveToNextChunkIfExhausted();
			return element;
		}

		protected abstract Iterator<T> createChunkElementIterator(final ChronoChunk chunk);
	}

	private class HistoryIterator extends LazyChunkIterator<Long> {

		private final long maxTime;
		private final String key;

		public HistoryIterator(final List<ChronoChunk> chunks, final long maxTime, final String key) {
			super(chunks.iterator());
			checkArgument(maxTime >= 0,
					"Precondition violation - argument 'maxTime' must be greater than or equal to zero!");
			checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
			this.maxTime = maxTime;
			this.key = key;
			this.moveToNextChunkIfExhausted();
		}

		@Override
		protected Iterator<Long> createChunkElementIterator(final ChronoChunk chunk) {
			try (TuplTransaction tx = TemporalChunkDbDataMatrix.this.chunkManager
					.openTransactionOn(chunk.getDataFile())) {
				return TuplDataMatrixUtil.history(tx, TemporalChunkDbDataMatrix.this.mapName,
						TemporalChunkDbDataMatrix.this.getKeyspace(), this.maxTime, this.key);
			}
		}
	}

	private class ModificationsIterator extends LazyChunkIterator<TemporalKey> {

		private Period period;

		public ModificationsIterator(final List<ChronoChunk> chunks, final Period period) {
			super(chunks.iterator());
			checkNotNull(period, "Precondition violation - argument 'period' must not be NULL!");
			this.period = period;
			this.moveToNextChunkIfExhausted();
		}

		@Override
		protected Iterator<TemporalKey> createChunkElementIterator(final ChronoChunk chunk) {
			long lowerBound = Math.max(this.period.getLowerBound(), chunk.getMetaData().getValidFrom());
			long upperBound = Math.min(this.period.getUpperBound(), chunk.getMetaData().getValidTo());
			try (TuplTransaction tx = TemporalChunkDbDataMatrix.this.chunkManager
					.openTransactionOn(chunk.getDataFile())) {
				return TuplDataMatrixUtil.getModificationsBetween(tx, TemporalChunkDbDataMatrix.this.mapName,
						TemporalChunkDbDataMatrix.this.getKeyspace(), lowerBound, upperBound);
			}
		}
	}

	private class AllEntriesIterator extends AbstractCloseableIterator<UnqualifiedTemporalEntry> {

		private final LazyChunkIterator<UnqualifiedTemporalEntry> innerIterator;
		private CloseableIterator<UnqualifiedTemporalEntry> currentIterator = null;

		private final long maxTimestamp;

		public AllEntriesIterator(final List<ChronoChunk> chunks, final long maxTimestamp) {
			checkNotNull(chunks, "Precondition violation - argument 'chunks' must not be NULL!");
			checkArgument(maxTimestamp >= 0,
					"Precondition violation - argument 'maxTimestamp' must be greater than or equal to zero!");
			this.innerIterator = new LazyChunkIterator<UnqualifiedTemporalEntry>(chunks.iterator()) {

				@Override
				protected Iterator<UnqualifiedTemporalEntry> createChunkElementIterator(final ChronoChunk chunk) {
					return AllEntriesIterator.this.createChunkElementIterator(chunk);
				}
			};
			this.maxTimestamp = maxTimestamp;
			this.innerIterator.moveToNextChunkIfExhausted();
		}

		@Override
		public UnqualifiedTemporalEntry next() {
			if (this.hasNext() == false) {
				throw new NoSuchElementException("Iterator is exhausted!");
			}
			return this.innerIterator.next();
		}

		@Override
		protected boolean hasNextInternal() {
			return this.innerIterator.hasNext();
		}

		@Override
		protected void closeInternal() {
			if (this.currentIterator != null) {
				this.currentIterator.close();
				this.currentIterator = null;
			}
		}

		protected Iterator<UnqualifiedTemporalEntry> createChunkElementIterator(final ChronoChunk chunk) {
			if (this.currentIterator != null) {
				this.currentIterator.close();
			}
			TuplTransaction tx = TemporalChunkDbDataMatrix.this.chunkManager
					.openBogusTransactionOn(chunk.getDataFile());
			this.currentIterator = TuplDataMatrixUtil.allEntriesIterator(tx, TemporalChunkDbDataMatrix.this.mapName,
					this.maxTimestamp);
			return this.currentIterator.asIterator();
		}

	}
}
