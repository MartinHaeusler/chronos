package org.chronos.chronodb.internal.impl.engines.chunkdb.index;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.exceptions.ChronoDBIndexingException;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChronoChunk;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChunkedChronoDB;
import org.chronos.chronodb.internal.impl.engines.tupl.NavigationIndex;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplDataMatrixUtil;
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.chronodb.internal.util.MultiMapUtil;
import org.chronos.chronodb.internal.util.concurrent.ResolvedFuture;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.util.LRUCacheUtil;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

public class IndexChunkManager {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final ChunkedChronoDB owningDB;

	private final LoadingCache<ChronoChunk, DocumentBasedChunkIndex> chunkToIndex;

	private final Lock indexLoadLock = new ReentrantLock(true);
	private final ReadWriteLock indexLoadProcessLock = new ReentrantReadWriteLock(true);
	private final Map<ChronoChunk, Future<DocumentBasedChunkIndex>> indicesBeingLoaded = Maps.newHashMap();

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public IndexChunkManager(final ChunkedChronoDB owningDB) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		this.owningDB = owningDB;
		int indexLruCacheSize = 20; // TODO make configurable
		this.chunkToIndex = LRUCacheUtil.build(indexLruCacheSize, this::loadChunkIndex);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	/**
	 * Returns the index for the given chunk.
	 *
	 * <p>
	 * Please note that this is a potentially expensive operation in the case that the given chunk has no persistent index.
	 *
	 * <p>
	 * <b>IMPORTANT NOTE:</b> Do not store the returned instance for extended periods of time, e.g. in fields. Re-fetch it as needed, the instances are cached. The reason is that the indices may be deleted via {@link #deleteIndexForChunk(ChronoChunk)}.
	 *
	 * @param chunk
	 *            The chunk to get the index for. Must not be <code>null</code>.
	 * @return The index for the chunk. Never <code>null</code>.
	 */
	public DocumentBasedChunkIndex getIndexForChunk(final ChronoChunk chunk) {
		Future<DocumentBasedChunkIndex> indexForChunk = this.getIndexForChunkAsFuture(chunk);
		try {
			return indexForChunk.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new ChronoDBIndexingException("Failed to fetch chunk index!", e);
		}
	}

	/**
	 * Returns the index for the given chunk, as a {@link Future}.
	 *
	 * <p>
	 * Please note that this is a potentially expensive operation in the case that the given chunk has no persistent index.
	 *
	 * <p>
	 * <b>IMPORTANT NOTE:</b> Do not store the returned instance for extended periods of time, e.g. in fields. Re-fetch it as needed, the instances are cached. The reason is that the indices may be deleted via {@link #deleteIndexForChunk(ChronoChunk)}.
	 *
	 *
	 * @param chunk
	 *            The chunk to get the index for. Must not be <code>null</code>.
	 *
	 * @return A future representing the index for the given chunk. Never <code>null</code>.
	 */
	public Future<DocumentBasedChunkIndex> getIndexForChunkAsFuture(final ChronoChunk chunk) {
		checkNotNull(chunk, "Precondition violation - argument 'chunk' must not be NULL!");
		FutureTask<DocumentBasedChunkIndex> task = null;
		this.indexLoadLock.lock();
		try {
			DocumentBasedChunkIndex index = this.chunkToIndex.getIfPresent(chunk);
			if (index != null) {
				// index is already loaded
				return new ResolvedFuture<>(index);
			}
			// index is not yet loaded; check if somebody else is loading it
			Future<DocumentBasedChunkIndex> future = this.indicesBeingLoaded.get(chunk);
			if (future != null) {
				// somebody else is loading this index, wait for it
				return future;
			}
			// nobody else is currently creating the index, so we do it ourselves.
			task = new FutureTask<>(() -> this.loadChunkIndex(chunk));
			this.indicesBeingLoaded.put(chunk, task);
		} finally {
			this.indexLoadLock.unlock();
		}
		// use the current thread to carry out the task
		task.run();
		return task;
	}

	/**
	 * Deletes all indices (both in-memory representation and files) for the given chunk.
	 *
	 * <p>
	 * Calling this method inevitably entails that the next call to {@link #getIndexForChunk(ChronoChunk)} (with the given chunk as argument) will have to re-create that index from scratch, which is an expensive operation. Use this method with care!
	 *
	 * <p>
	 * If the given chunk has no index, this method does nothing.
	 *
	 * @param chunk
	 *            The chunk to delete the index for. Must not be <code>null</code>.
	 */
	public void deleteIndexForChunk(final ChronoChunk chunk) {
		checkNotNull(chunk, "Precondition violation - argument 'chunk' must not be NULL!");
		this.indexLoadLock.lock();
		this.indexLoadProcessLock.writeLock().lock();
		try {
			this.chunkToIndex.invalidate(chunk);
			chunk.deleteIndexFile();
		} finally {
			this.indexLoadProcessLock.writeLock().unlock();
			this.indexLoadLock.unlock();
		}
	}

	/**
	 * Deletes all chunk indices (both in-memory representation and files).
	 *
	 * <p>
	 * Calling this method inevitably entails that the next call to {@link #getIndexForChunk(ChronoChunk)} will have to re-create that index from scratch, which is an expensive operation. Use this method with care!
	 *
	 * <p>
	 * If no indices exist, this method does nothing.
	 */
	public void deleteAllChunkIndices() {
		this.indexLoadLock.lock();
		this.indexLoadProcessLock.writeLock().lock();
		try {
			this.chunkToIndex.invalidateAll();
			this.owningDB.getChunkManager().dropChunkIndexFiles();
		} finally {
			this.indexLoadProcessLock.writeLock().unlock();
			this.indexLoadLock.unlock();
		}
	}

	/**
	 * Rolls back the contents of chunk indices.
	 *
	 * @param branchNames
	 *            The branches to roll back. Must not be <code>null</code>. If this is set is empty, this method returns immediately without performing any operations.
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative. Must be within the validity period of the last chunk.
	 */
	public void rollbackChunkIndices(final Set<String> branchNames, final long timestamp) {
		this.rollbackChunkIndices(branchNames, timestamp, null);
	}

	/**
	 * Rolls back the contents of chunk indices.
	 *
	 * <p>
	 * This is possible only for indices of chunks that are open-ended, i.e. the last chunk in each branch. This method has no effect on intermediate chunks.
	 *
	 * @param branches
	 *            The branches to consider for rollback. Must not be <code>null</code>. If this is set is empty, this method returns immediately without performing any operations.
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative. Must be within the validity period of the last chunk.
	 * @param keys
	 *            The set of keys to roll back. If this is the empty set, no key will be rolled back and this method returns immediately. If this is <code>null</code>, then all keys will be rolled back.
	 */
	public void rollbackChunkIndices(final Set<String> branches, final long timestamp, final Set<QualifiedKey> keys) {
		checkNotNull(branches, "Precondition violation - argument 'branches' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		if (branches.isEmpty() || keys != null && keys.isEmpty()) {
			// either no branch to roll back, or keys to roll back is empty -> no work to do
			return;
		}
		this.indexLoadLock.lock();
		this.indexLoadProcessLock.writeLock().lock();
		try {
			for (Entry<ChronoChunk, DocumentBasedChunkIndex> entry : this.chunkToIndex.asMap().entrySet()) {
				ChronoChunk chunk = entry.getKey();
				DocumentBasedChunkIndex index = entry.getValue();
				if (branches.contains(chunk.getBranchName()) == false) {
					// this branch is not affected by the rollback
					continue;
				}
				Period validPeriod = chunk.getMetaData().getValidPeriod();
				if (validPeriod.contains(timestamp) == false) {
					continue;
				}
				if (validPeriod.isOpenEnded() == false) {
					ChronoLogger.logWarning("Can not roll back index for branch '" + chunk.getBranchName()
							+ "' to timestamp '" + timestamp
							+ "', because this timestamp does not belong to the HEAD chunk! The rollback on the index of this chunk will not be performed.");
					continue;
				}
				if (index.isPersistent()) {
					// cannot roll back persistent indices
					continue;
				}
				index.rollback(branches, timestamp, keys);
			}
		} finally {
			this.indexLoadProcessLock.writeLock().unlock();
			this.indexLoadLock.unlock();
		}
	}

	/**
	 * Deletes the contents of the given index, for all branches.
	 *
	 * <p>
	 * For indices that are being held in memory, this means that the index with the given name will be deleted. For persistent indices, this will result in the deletion of the entire index (file), which will entail a full re-index on the next request.
	 *
	 * @param indexName
	 *            The index to delete. Must not be <code>null</code>.
	 */
	public void deleteIndexContents(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.indexLoadLock.lock();
		this.indexLoadProcessLock.writeLock().lock();
		try {
			Map<ChronoChunk, DocumentBasedChunkIndex> indices = Maps.newHashMap(this.chunkToIndex.asMap());
			for (Entry<ChronoChunk, DocumentBasedChunkIndex> entry : indices.entrySet()) {
				ChronoChunk chunk = entry.getKey();
				DocumentBasedChunkIndex index = entry.getValue();
				if (index.isPersistent()) {
					// we need to evict this index from our cache and delete the index file
					this.chunkToIndex.invalidate(chunk);
					chunk.deleteIndexFile();
				} else {
					// update the index in-memory
					index.deleteIndexContents(indexName);
				}
			}
		} finally {
			this.indexLoadProcessLock.writeLock().unlock();
			this.indexLoadLock.unlock();
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	/**
	 * Performs the actual loading work for the index of the given chunk.
	 *
	 * <p>
	 * This is a <b>potentially expensive</b> procedure (CPU, RAM, Disk I/O...). If there is an index file for the given chunk, this method should be quite efficient. However, if iteration over the primary index is required to reconstruct the secondary index, then this method will be expensive to compute.
	 *
	 * <p>
	 * The strategy is as follows:
	 * <ol>
	 * <li>Try to load the index from an existing index file.
	 * <li>If the index file exists and is correct, load it via deserialization.
	 * <li>If the file is corrupt, delete it.
	 * <li>If we managed to load the index, we return it and are done.
	 * <li>Otherwise, we create the index for the chunk from scratch by consulting the primary index. Then, save the result to disk and return the computed index.
	 * </ol>
	 *
	 * @param chunk
	 *            The chunk to load the index for. Must not be <code>null</code>.
	 *
	 * @return The index manager backend for the given chunk. Never <code>null</code>.
	 */
	private DocumentBasedChunkIndex loadChunkIndex(final ChronoChunk chunk) {
		DocumentBasedChunkIndex index = null;
		this.indexLoadProcessLock.readLock().lock();
		try {
			// try to find the index data on disk
			if (chunk.hasIndexFile()) {
				File indexFile = chunk.getIndexFile();
				if (indexFile.length() > 0) {
					// index file exists; try to load it
					try {
						// try to load the raw data
						DocumentChunkIndexData indexData = DocumentChunkIndexData.loadFromFile(indexFile);
						if (indexData == null) {
							// load failed
							index = null;
						} else {
							// load successful; transform it into an in-memory index
							index = new DocumentBasedChunkIndex(this.owningDB, indexData);
						}
					} catch (Exception e) {
						ChronoLogger.logWarning("Failed to read contents of index file '" + indexFile.getAbsolutePath()
								+ "'! Will re-create this index from scratch. Root cause: " + e.toString());
						index = null;
					}
					if (index == null) {
						// the index file either doesn't exist, or is corrupted...
						chunk.deleteIndexFile();
					}
				}
			}
			if (index == null) {
				// no index is present; we need to create it from scratch
				index = this.createChunkIndexFromScratch(chunk);
			}
			return index;
		} finally {
			this.indexLoadProcessLock.readLock().unlock();
			this.indexLoadLock.lock();
			try {
				// we are done loading this index
				this.indicesBeingLoaded.remove(chunk);
				if (index != null) {
					this.chunkToIndex.put(chunk, index);
				}
			} finally {
				this.indexLoadLock.unlock();
			}
		}
	}

	/**
	 * Creates a {@link DocumentBasedChunkIndex} for the given chunk from scratch.
	 *
	 * <p>
	 * This is an <b>expensive</b> procedure (CPU, RAM, Disk I/O...) that should be avoided unless strictly necessary. Cases where this method should be called include:
	 * <ul>
	 * <li>Loss of index chunk (file was deleted or corrupted)
	 * <li>Change in indexers (reindexing)
	 * </ul>
	 *
	 * @param chunk
	 *            The chunk to create the index for. Must not be <code>null</code>.
	 *
	 * @return The index manager backend. Never <code>null</code>.
	 */
	private DocumentBasedChunkIndex createChunkIndexFromScratch(final ChronoChunk chunk) {
		// prepare some data we need later on
		String branchName = chunk.getBranchName();
		// System.out.println("IndexChunkManager :: loading index for Chunk '" + branchName + "#"
		// + chunk.getSequenceNumber() + "', period: " + chunk.getMetaData().getValidPeriod());
		// get access to the indexers
		Map<String, Set<Indexer<?>>> indexNameToIndexer = this.owningDB.getIndexManager().getIndexersByIndexName();
		// prepare the document container
		DocumentChunkIndexData indexData = new DocumentChunkIndexData();
		indexData.setBranchName(branchName);
		indexData.setIndexers(indexNameToIndexer);
		// make sure that an index file exists
		chunk.createIndexFileIfNotExists();
		File indexFile = chunk.getIndexFile();
		if (this.isBranchDeltaChunk(chunk)) {
			// create the "baseline" by iterating over the entry set of the origin branch, then
			// continue by adding in the delta from the chunk
			indexData.addIndexDocuments(this.createIndexDocumentsForDeltaChunk(chunk));
		} else {
			// create the documents by iterating over the contents of the chunk
			indexData.addIndexDocuments(this.createIndexDocumentsForRegularChunk(chunk));
		}
		// System.out.println("IndexChunkManager :: created index with " + indexData.getIndexDocuments().size()
		// + " index documents for Chunk '" + branchName + "#" + chunk.getSequenceNumber() + "', period: "
		// + chunk.getMetaData().getValidPeriod());
		// create the actual index
		DocumentBasedChunkIndex index = new DocumentBasedChunkIndex(this.owningDB, indexData);
		// if the chunk is "closed" (i.e. has a fixed end time), then we also want to persist the index
		if (chunk.getMetaData().getValidPeriod().isOpenEnded() == false) {
			// write out the chunk for later reference
			DocumentChunkIndexData.persistToFile(indexFile, indexData);
			// remember that we have persisted this index
			index.setPersistent(true);
		}
		return index;
	}

	/**
	 * Checks if the given chunk is the first in the chunk series for a non-master branch.
	 *
	 * <p>
	 * Those chunks contain the delta to their origin branch at the branching timestamp. After the first rollover, they will contain the full information.
	 *
	 * @param chunk
	 *            The chunk to check. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if the given chunk is a delta chunk, i.e. the first chunk after a branching operation, otherwise <code>false</code>.
	 */
	private boolean isBranchDeltaChunk(final ChronoChunk chunk) {
		Period validPeriod = chunk.getMetaData().getValidPeriod();
		String branchName = chunk.getBranchName();
		Branch branch = this.owningDB.getBranchManager().getBranch(branchName);
		if (ChronoDBConstants.MASTER_BRANCH_IDENTIFIER.equals(branchName)) {
			// the master branch has no delta chunks
			return false;
		}
		// if our validity period contains the branching timestamp, then the chunk is the first in the series
		if (validPeriod.contains(branch.getBranchingTimestamp())) {
			// the chunk is the first in this branch and contains the delta to the origin
			return true;
		} else {
			// the chunk has been rolled over at least once and contains the complete information
			return false;
		}
	}

	/**
	 * Creates the full list of index documents for the given "delta" chunk.
	 *
	 * <p>
	 * A "delta" chunk is a chunk that:
	 * <ul>
	 * <li>is not part of the {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch, AND
	 * <li>is the first chunk in a branch
	 * </ul>
	 *
	 * <p>
	 * The critical property of a "regular" chunk is that it does not contain an entire snapshot of the contents, i.e. it is not self-contained. The opposite of a "delta" chunk is a "regular" chunk, which can be indexed via {@link #createIndexDocumentsForRegularChunk(ChronoChunk)}.
	 *
	 * @param chunk
	 *            The chunk to index. Must not be <code>null</code>.
	 *
	 * @return The list of index documents extracted from this chunk (in no particular order). May be empty, but never <code>null</code>.
	 */
	private List<ChunkDbIndexDocumentData> createIndexDocumentsForDeltaChunk(final ChronoChunk chunk) {
		checkNotNull(chunk, "Precondition violation - argument 'chunk' must not be NULL!");
		String branchName = chunk.getBranchName();
		Branch branch = this.owningDB.getBranchManager().getBranch(branchName);
		Period chunkPeriod = chunk.getMetaData().getValidPeriod();
		Set<KeyspaceMetadata> allKeyspaceMetadata = null;
		try (TuplTransaction tx = this.owningDB.openTx()) {
			allKeyspaceMetadata = NavigationIndex.getKeyspaceMetadata(tx, branchName);
			tx.commit();
		}
		if (allKeyspaceMetadata == null) {
			throw new IllegalStateException("Could not read Keyspace Metadata for Branch '" + branchName + "'!");
		}
		// create the document list builder that will be passed to the indexing methods
		DocumentListBuilder documentListBuilder = new DocumentListBuilder();
		// fetch the indexers from our owning DB
		SetMultimap<String, Indexer<?>> indexerMultimap = MultiMapUtil
				.copyToMultimap(this.owningDB.getIndexManager().getIndexersByIndexName());
		// iterate over all keyspaces
		for (KeyspaceMetadata keyspaceMetadata : allKeyspaceMetadata) {
			// check if the keyspace exists in our period
			long creationTimestamp = keyspaceMetadata.getCreationTimestamp();
			String keyspace = keyspaceMetadata.getKeyspaceName();
			Period keyspaceExistencePeriod = Period.createOpenEndedRange(creationTimestamp);
			if (chunkPeriod.overlaps(keyspaceExistencePeriod) == false) {
				// the keyspace was created after the period we are interested in; skip it
				continue;
			}
			// fetch the entry set of this keyspace (this will traverse the branching hierarchy backwards to origin)
			long timestamp = chunkPeriod.getLowerBound();
			ChronoDBTransaction tx = this.owningDB.tx(branch.getOrigin().getName(), branch.getBranchingTimestamp());
			// TODO Performance: using 'entrySet(...)' here would be much more efficient; doesn't exist in public API
			// yet
			Set<String> keySet = tx.keySet(keyspace);
			for (String key : keySet) {
				Object object = tx.get(keyspace, key);
				if (object == null) {
					// as we build a new index for this chunk, we are not interested in elements that were deleted
					// before this chunk even started to exist.
					continue;
				}
				// index the given value, creating new documents
				SetMultimap<String, Object> indexedValues = IndexingUtils.getIndexedValuesForObject(indexerMultimap,
						object);
				for (Entry<String, Object> indexEntry : indexedValues.entries()) {
					String indexName = indexEntry.getKey();
					Object value = indexEntry.getValue();
					ChunkDbIndexDocumentData document = new ChunkDbIndexDocumentData(indexName, keyspace, key, value,
							timestamp);
					documentListBuilder.addDocument(document);
				}
			}
		}
		// now we have our "baseline" of documents (i.e. the documents that existed in the origin branch at the
		// branching timestamp).
		// We continue by considering the contents of our branch chunk.
		this.createIndexDocumentsForChunk(chunk, documentListBuilder);
		// all keyspaces have been indexed; transform the builder into a regular list of documents
		return documentListBuilder.getAllDocumentsAndClose();
	}

	/**
	 * Creates the full list of index documents for the given "regular" chunk.
	 *
	 * <p>
	 * A "regular" chunk is a chunk that:
	 * <ul>
	 * <li>is part of the {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch, OR
	 * <li>is not the first chunk in a branch.
	 * </ul>
	 *
	 * <p>
	 * The critical property of a "regular" chunk is that it contains an entire snapshot of the contents, i.e. it is self-contained. The opposite of a "regular" chunk is a "delta" chunk, which can be indexed via {@link #createIndexDocumentsForDeltaChunk(ChronoChunk)}.
	 *
	 * @param chunk
	 *            The chunk to index. Must not be <code>null</code>.
	 *
	 * @return The list of index documents extracted from this chunk (in no particular order). May be empty, but never <code>null</code>.
	 */
	private List<ChunkDbIndexDocumentData> createIndexDocumentsForRegularChunk(final ChronoChunk chunk) {
		checkNotNull(chunk, "Precondition violation - argument 'chunk' must not be NULL!");
		// for a regular chunk, we only need to iterate over the contents of the chunk and calculate the index. There
		// is no need to consider any other chunks (as it is the case for delta chunks).
		DocumentListBuilder builder = new DocumentListBuilder();
		this.createIndexDocumentsForChunk(chunk, builder);
		return builder.getAllDocumentsAndClose();
	}

	/**
	 * Creates the full list of index documents for the given chunk and fills them into the given document list builder.
	 *
	 * <p>
	 * Please note that this method will only consider the contents of the given chunk, i.e. it will not consider any other chunks in the branch sequence, or the origin chunk (if any).
	 *
	 * @param chunk
	 *            The chunk to index. Must not be <code>null</code>.
	 * @param documentListBuilder
	 *            The document list builder to fill the index documents into. Must not be <code>null</code>.
	 */
	private void createIndexDocumentsForChunk(final ChronoChunk chunk, final DocumentListBuilder documentListBuilder) {
		checkNotNull(chunk, "Precondition violation - argument 'chunk' must not be NULL!");
		checkNotNull(documentListBuilder, "Precondition violation - argument 'documentListBuilder' must not be NULL!");
		// fetch some information that we are going to need later on
		String branchName = chunk.getBranchName();
		Period chunkPeriod = chunk.getMetaData().getValidPeriod();
		Set<KeyspaceMetadata> allKeyspaceMetadata = null;
		try (TuplTransaction tx = this.owningDB.openTx()) {
			allKeyspaceMetadata = NavigationIndex.getKeyspaceMetadata(tx, branchName);
			tx.commit();
		}
		if (allKeyspaceMetadata == null) {
			throw new IllegalStateException("Could not read Keyspace Metadata for Branch '" + branchName + "'!");
		}
		// iterate over all keyspaces
		for (KeyspaceMetadata keyspaceMetadata : allKeyspaceMetadata) {
			// check if the keyspace exists in our period
			long creationTimestamp = keyspaceMetadata.getCreationTimestamp();
			Period keyspaceExistencePeriod = Period.createOpenEndedRange(creationTimestamp);
			if (chunkPeriod.overlaps(keyspaceExistencePeriod) == false) {
				// the keyspace was created after the period we are interested in; skip it
				continue;
			}
			// extract the information required to get access to the underlying MapDB map
			String keyspaceName = keyspaceMetadata.getKeyspaceName();
			String matrixName = keyspaceMetadata.getMatrixTableName();
			// calculate the upper bound of the timestamps we consider, which is either
			// the "now" timestamp or the chunk validity upper bound, whichever is smaller
			long now = this.owningDB.getBranchManager().getBranch(branchName).getNow();
			long upperTimestampBound = Math.min(now, chunkPeriod.getUpperBound());

			// access the chunk
			try (TuplTransaction tx = this.owningDB.getChunkManager().openBogusTransactionOn(chunk.getDataFile())) {
				// iterate over all entries
				CloseableIterator<UnqualifiedTemporalEntry> allEntriesCIterator = TuplDataMatrixUtil
						.allEntriesIterator(tx, matrixName, upperTimestampBound);
				try {
					Iterator<UnqualifiedTemporalEntry> allEntriesIterator = allEntriesCIterator.asIterator();
					this.indexKeyspaceContents(keyspaceName, allEntriesIterator, documentListBuilder);
				} finally {
					allEntriesCIterator.close();
				}
			}
		}
	}

	/**
	 * Indexes the contents of the given keyspace and fills the resulting documents into the given document list builder.
	 *
	 * @param keyspaceName
	 *            The name of the keyspace to index. Must not be <code>null</code>.
	 * @param keyspaceEntryIterator
	 *            The iterator over the keyspace contents. Must not be <code>null</code>. The iteration order is assumed to be "first by key, then by timestamp ascending".
	 * @param documentListBuilder
	 *            The document list builder that manages the produced documents. Must not be <code>null</code>. Will be filled during the execution of this method with the documents extracted from the given entry iterator.
	 */
	private void indexKeyspaceContents(final String keyspaceName,
			final Iterator<UnqualifiedTemporalEntry> keyspaceEntryIterator,
			final DocumentListBuilder documentListBuilder) {
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(keyspaceEntryIterator,
				"Precondition violation - argument 'keyspaceEntryIterator' must not be NULL!");
		checkNotNull(documentListBuilder, "Precondition violation - argument 'documentListBuilder' must not be NULL!");
		SerializationManager serializationManager = this.owningDB.getSerializationManager();
		SetMultimap<String, Indexer<?>> indexerMultimap = MultiMapUtil
				.copyToMultimap(this.owningDB.getIndexManager().getIndexersByIndexName());
		while (keyspaceEntryIterator.hasNext()) {
			UnqualifiedTemporalEntry entry = keyspaceEntryIterator.next();
			byte[] serializedValue = entry.getValue();
			Object newValue = null;
			String key = entry.getKey().getKey();
			long timestamp = entry.getKey().getTimestamp();
			if (serializedValue != null && serializedValue.length > 0) {
				newValue = serializationManager.deserialize(serializedValue);
			}
			if (newValue == null) {
				// deletion; terminate all documents that belong to this qualified key
				Set<ChunkDbIndexDocumentData> openDocuments = documentListBuilder.getOpenDocuments(keyspaceName, key);
				for (ChunkDbIndexDocumentData doc : openDocuments) {
					documentListBuilder.terminateDocumentValidity(doc, timestamp);
				}
			} else {
				// addition or update; create the index values
				SetMultimap<String, Object> indexNameToValues = IndexingUtils.getIndexedValuesForObject(indexerMultimap,
						newValue);
				// for all open documents that belong to this entry, check if they are
				// still valid, terminate them if not, and create new documents for
				// all new index values
				Set<ChunkDbIndexDocumentData> openDocuments = documentListBuilder.getOpenDocuments(keyspaceName, key);
				for (ChunkDbIndexDocumentData doc : openDocuments) {
					String indexName = doc.getIndexName();
					Object indexValue = doc.getIndexedValue();
					if (indexNameToValues.containsEntry(indexName, indexValue)) {
						// document continues to be valid, no need to create a new one
						indexNameToValues.remove(indexName, indexValue);
					} else {
						// document is no longer valid because the value is gone
						documentListBuilder.terminateDocumentValidity(doc, timestamp);
					}
				}
				// all entries that remain in our index value map have no document yet,
				// so we create one for each of them.
				for (Entry<String, Object> indexNameToValue : indexNameToValues.entries()) {
					String indexName = indexNameToValue.getKey();
					Object indexValue = indexNameToValue.getValue();
					ChunkDbIndexDocumentData doc = new ChunkDbIndexDocumentData(indexName, keyspaceName,
							entry.getKey().getKey(), indexValue, entry.getKey().getTimestamp());
					documentListBuilder.addDocument(doc);
				}
			}
		}
	}

}
