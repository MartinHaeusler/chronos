package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChangeSetEntry;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.exceptions.BlindOverwriteException;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal.ChronoNonReturningJob;
import org.chronos.chronodb.internal.api.ChronoDBInternal.ChronoReturningJob;
import org.chronos.chronodb.internal.api.CommitMetadataStore;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.RangedGetResult;
import org.chronos.chronodb.internal.api.TemporalDataMatrix;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.serialization.KryoManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public abstract class AbstractTemporalKeyValueStore extends TempoalKeyValueStoreBase implements TemporalKeyValueStore {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	/**
	 * The branch lock protects a single branch from illegal concurrent access.
	 * <p>
	 * The vast majority of requests will only require a read lock, including (but not limited to):
	 * <ul>
	 * <li>Read operations
	 * <li>Commits
	 * </ul>
	 *
	 * An example for a process which does indeed require the write lock (i.e. exclusive lock) is a re-indexing process,
	 * as index values will be invalid during this process, which makes reads pointless.
	 *
	 * <p>
	 * Please note that acquiring any lock (read or write) on a branch is legal if and only if the currrent thread holds
	 * a read lock on the owning database as well.
	 */
	private final ReadWriteLock branchLock = new ReentrantReadWriteLock(true);

	/**
	 * The commit lock is a plain reentrant lock that protects a single branch from concurrent commits.
	 *
	 * <p>
	 * Please note that this lock may be acquired if and only if the current thread is holding all of the following
	 * locks:
	 * <ul>
	 * <li>Database lock (read or write)
	 * <li>Branch lock (read or write)
	 * </ul>
	 *
	 * Also note that (as the name implies) this lock is for commit operations only. Read operations do not need to
	 * acquire this lock at all.
	 */
	private final Lock commitLock = new ReentrantLock(true);

	private final BranchInternal owningBranch;
	private final ChronoDBInternal owningDB;
	protected final Map<String, TemporalDataMatrix> keyspaceToMatrix = Maps.newHashMap();

	private ChronoDBCache cache = null;

	/** This lock is used to protect incremental commit data from illegal concurrent access. */
	protected final Lock incrementalCommitLock = new ReentrantLock(true);
	/** This field is used to keep track of the transaction that is currently executing an incremental commit. */
	protected ChronoDBTransaction incrementalCommitTransaction = null;
	/** This timestamp will be written to during incremental commits, all of them will write to this timestamp. */
	protected long incrementalCommitTimestamp = -1L;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected AbstractTemporalKeyValueStore(final ChronoDBInternal owningDB, final BranchInternal owningBranch) {
		checkNotNull(owningBranch, "Precondition violation - argument 'owningBranch' must not be NULL!");
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		this.owningDB = owningDB;
		this.owningBranch = owningBranch;
		this.owningBranch.setTemporalKeyValueStore(this);
	}

	// =================================================================================================================
	// PUBLIC API (common implementations)
	// =================================================================================================================

	@Override
	public void performStartupRecoveryIfRequired() {
		WriteAheadLogToken walToken = this.getWriteAheadLogTokenIfExists();
		if (walToken == null) {
			// we have no Write-Ahead-Log token. This means that there was no ongoing commit
			// during the shutdown of the database. Therefore, no recovery is required.
			return;
		}
		ChronoLogger.logWarning(
				"There has been an error during the last shutdown. ChronoDB will attempt to recover to the last consistent state (this may take a few minutes).");
		// we have a WAL-Token, so we need to perform a recovery. We roll back to the
		// last valid timestamp before the commit (which was interrupted by JVM shutdown) has occurred.
		long timestamp = walToken.getNowTimestampBeforeCommit();
		// we must assume that all keyspaces were modified (in the worst case)
		Set<String> modifiedKeyspaces = this.getAllKeyspaces();
		// we must also assume that our index is broken (in the worst case)
		boolean touchedIndex = true;
		// perform the rollback
		this.performRollbackToTimestamp(timestamp, modifiedKeyspaces, touchedIndex);
		this.clearWriteAheadLogToken();
	}

	@Override
	public Branch getOwningBranch() {
		return this.owningBranch;
	}

	@Override
	public ChronoDBInternal getOwningDB() {
		return this.owningDB;
	}

	public Set<String> getAllKeyspaces() {
		return this.performNonExclusive(() -> {
			// produce a duplicate of the set, because the actual key set changes over time and may lead to
			// unexpected "ConcurrentModificationExceptions" in the calling code when used for iteration purposes.
			Set<String> keyspaces = Sets.newHashSet(this.keyspaceToMatrix.keySet());
			// add the keyspaces of the origin recursively (if there is an origin)
			if (this.isMasterBranchTKVS() == false) {
				ChronoDBTransaction parentTx = this.getOriginBranchTKVS()
						.tx(this.getOwningBranch().getBranchingTimestamp());
				keyspaces.addAll(this.getOriginBranchTKVS().getKeyspaces(parentTx));
			}
			return Collections.unmodifiableSet(keyspaces);
		});
	}

	@Override
	public Set<String> getKeyspaces(final ChronoDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return this.getKeyspaces(tx.getTimestamp());
	}

	@Override
	public Set<String> getKeyspaces(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return this.performNonExclusive(() -> {
			// produce a duplicate of the set, because the actual key set changes over time and may lead to
			// unexpected "ConcurrentModificationExceptions" in the calling code when used for iteration purposes.
			Set<String> keyspaces = Sets.newHashSet();
			for (Entry<String, TemporalDataMatrix> entry : this.keyspaceToMatrix.entrySet()) {
				String keyspaceName = entry.getKey();
				TemporalDataMatrix matrix = entry.getValue();
				if (matrix.getCreationTimestamp() <= timestamp) {
					keyspaces.add(keyspaceName);
				}
			}
			// add the keyspaces of the origin recursively (if there is an origin)
			if (this.isMasterBranchTKVS() == false) {
				long branchingTimestamp = this.getOwningBranch().getBranchingTimestamp();
				keyspaces.addAll(this.getOriginBranchTKVS().getKeyspaces(branchingTimestamp));
			}
			return Collections.unmodifiableSet(keyspaces);
		});
	}

	@Override
	public Set<String> getAllKeys(final String keyspace) {
		return this.performNonExclusive(() -> {
			Set<String> allKeys = Sets.newHashSet();
			TemporalDataMatrix matrix = this.getMatrix(keyspace);
			if (matrix != null) {
				Iterator<String> iterator = matrix.allKeys();
				Iterators.addAll(allKeys, iterator);
			}
			if (this.isMasterBranchTKVS() == false) {
				allKeys.addAll(this.getOriginBranchTKVS().getAllKeys(keyspace));
			}
			return allKeys;
		});
	}

	@Override
	public ChronoDBCache getCache() {
		return this.performNonExclusive(() -> {
			return this.cache;
		});
	}

	@Override
	public void setCache(final ChronoDBCache cache) {
		this.performBranchExclusive(() -> {
			if (cache != null) {
				cache.clear();
			}
			this.cache = cache;
		});
	}

	@Override
	public void performCommit(final ChronoDBTransaction tx, final Object commitMetadata) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		this.assertThatTransactionMayPerformCommit(tx);
		// Note: the locking process here is special. We acquire the following locks (in this order):
		//
		// 1) DB Read Lock
		// Reason: Writing on one branch does not need to block the others. The case that simultaneous writes
		// occur on the same branch is handled by different locks. The DB Write Lock is only intended for very
		// drastic operations, such as branch removal or DB dump reading.
		//
		// 2) Branch Read Lock
		// Reason: We only acquire the read lock on the branch. This is because we want read transactions to be
		// able to continue reading, even though we are writing (on a different version). The branch write lock
		// is intended only for drastic operations that prevent reading, such as reindexing.
		//
		// 3) Commit Lock
		// Reason: This is not a read-write lock, this is a plain old lock. It prevents concurrent writes on the
		// same branch. Read operations never acquire this lock.

		// TODO CORRECTNESS MAPDB: This might be a problem without transactions. See explanation below.
		// The difficult case here is when there are multiple branches, and multiple threads are writing to
		// different branches at the same time. This should be allowed. However, as there is only a single
		// MapDB in the backend, we have unprotected concurrent write access on it. There are two solutions
		// here:
		// - Lock the entire DB (all branches) when committing. Simple, but not very nice.
		// - Have a MapDB instance per branch. Nicer, but a bit more complicated.
		this.performBranchExclusive(() -> {
			this.commitLock.lock();
			try {
				if (this.isIncrementalCommitProcessOngoing() && tx.getChangeSet().isEmpty() == false) {
					// "terminate" the incremental commit process with a FINAL incremental commit,
					// then continue with a true commit that has an EMPTY change set
					tx.commitIncremental();
				}

				Set<ChangeSetEntry> changeSet = tx.getChangeSet();
				if (this.isIncrementalCommitProcessOngoing() == false) {
					if (changeSet.isEmpty()) {
						// change set is empty -> there is nothing to commit
						return;
					}
				}
				long time = 0;
				if (this.isIncrementalCommitProcessOngoing()) {
					// use the incremental commit timestamp
					time = this.incrementalCommitTimestamp;
				} else {
					// use the current transaction time
					time = System.currentTimeMillis();
					// make sure we do not write to the same timestamp twice
					while (time <= this.getNow()) {
						try {
							Thread.sleep(1);
						} catch (InterruptedException ignored) {
						}
						time = System.currentTimeMillis();
					}
				}

				Map<String, Map<String, byte[]>> keyspaceToKeyToValue = Maps.newHashMap();
				Map<ChronoIdentifier, Pair<Object, Object>> entriesToIndex = Maps.newHashMap();
				for (ChangeSetEntry entry : changeSet) {
					String keyspace = entry.getKeyspace();
					String key = entry.getKey();
					Object oldValue = tx.get(keyspace, key);
					Object newValue = entry.getValue();
					if (tx.getConfiguration().getDuplicateVersionEliminationMode()
							.equals(DuplicateVersionEliminationMode.ON_COMMIT)) {
						if (Objects.equal(oldValue, newValue)) {
							// the new value is identical to the old one -> ignore it
							continue;
						}
					}
					Set<PutOption> options = entry.getOptions();
					Map<String, byte[]> keyspaceMap = keyspaceToKeyToValue.get(keyspace);
					if (keyspaceMap == null) {
						keyspaceMap = Maps.newHashMap();
						keyspaceToKeyToValue.put(keyspace, keyspaceMap);
					}
					if (entry.isRemove()) {
						keyspaceMap.put(key, null);
					} else {
						byte[] serialForm = this.getOwningDB().getSerializationManager().serialize(newValue);
						keyspaceMap.put(key, serialForm);
					}
					ChronoIdentifier identifier = ChronoIdentifier.create(this.getOwningBranch(), time, keyspace, key);
					if (options.contains(PutOption.NO_INDEX) == false) {
						entriesToIndex.put(identifier, Pair.of(oldValue, newValue));
					}
				}
				if (this.isIncrementalCommitProcessOngoing() == false) {
					// these features are not supported in incremental commit mode
					if (tx.getConfiguration().isBlindOverwriteProtectionEnabled()) {
						this.preventBlindOverwrite(tx, keyspaceToKeyToValue);
					}
					// before we begin the writing to disk, we store a token as a file. This token
					// will allow us to recover on the next startup in the event that the JVM crashes or
					// is being shut down during the commit process.
					WriteAheadLogToken token = new WriteAheadLogToken(this.getNow(), time);
					this.performWriteAheadLog(token);
				}
				// remember if we started to work with the index
				boolean touchedIndex = false;
				if (this.isIncrementalCommitProcessOngoing()) {
					// when committing incrementally, always assume that we touched the index, because
					// some of the preceeding incremental commits has very likely touched it.
					touchedIndex = true;
				}
				try {
					// here, we perform the actual *write* work.
					for (Entry<String, Map<String, byte[]>> entry : keyspaceToKeyToValue.entrySet()) {
						String keyspace = entry.getKey();
						Map<String, byte[]> contents = entry.getValue();
						TemporalDataMatrix matrix = this.getOrCreateMatrix(keyspace, time);
						matrix.put(time, contents);
					}
					IndexManager indexManager = this.getOwningDB().getIndexManager();
					if (indexManager != null) {
						touchedIndex = true;
						if (this.isIncrementalCommitProcessOngoing()) {
							// clear the query cache (if any). The reason for this is that during incremental upates,
							// we can get different results for the same query on the same timestamp. This is due to
							// changes in the same key at the timestamp of the incremental commit process.
							indexManager.clearQueryCache();
							// roll back the changed keys to the state before the incremental commit started
							Set<QualifiedKey> modifiedKeys = entriesToIndex.keySet().stream()
									.map(id -> QualifiedKey.create(id.getKeyspace(), id.getKey()))
									.collect(Collectors.toSet());
							indexManager.rollback(this.getOwningBranch(), this.getNow(), modifiedKeys);
							// re-index the modified keys
							indexManager.index(entriesToIndex);
						} else {
							// only index the new entries
							indexManager.index(entriesToIndex);
						}
					}
					// write the commit metadata object (if any)
					if (commitMetadata != null) {
						this.getCommitMetadataStore().put(time, commitMetadata);
					}

					// update the cache (if any)
					if (this.cache != null && this.isIncrementalCommitProcessOngoing()) {
						this.cache.rollbackToTimestamp(this.getNow());
					}
					this.writeCommitThroughCache(tx, time);
					this.setNow(time);
				} catch (Throwable t) {
					// an error occurred, we need to perform the rollback
					this.performRollbackToTimestamp(this.getNow(), keyspaceToKeyToValue.keySet(), touchedIndex);
					if (this.isIncrementalCommitProcessOngoing()) {
						// as a safety measure, we also have to clear the cache
						if (this.cache != null) {
							this.cache.clear();
						}
						// terminate the incremental commit process
						this.terminateIncrementalCommitProcess();
					}
					// after rolling back, we can clear the write ahead log
					this.clearWriteAheadLogToken();
					// throw the commit exception
					throw new ChronoDBCommitException(
							"An error occurred during the commit. Please see root cause for details.", t);
				}
				// everything ok in this commit, we can clear the write ahead log
				this.clearWriteAheadLogToken();
			} finally {
				if (this.isIncrementalCommitProcessOngoing()) {
					this.terminateIncrementalCommitProcess();
				}
				// drop the kryo instance we have been using, as it has some internal caches that just consume memory
				KryoManager.destroyKryo();
				this.commitLock.unlock();
			}
		});
	}

	@Override
	public long performCommitIncremental(final ChronoDBTransaction tx) throws ChronoDBCommitException {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		// make sure that this transaction may start (or continue with) an incremental commit process
		this.assertThatTransactionMayPerformIncrementalCommit(tx);
		// set up the incremental commit process, if this is the first incremental commit
		if (this.isFirstIncrementalCommit(tx)) {
			this.setUpIncrementalCommit(tx);
			// store the WAL token. We will need it to recover if the JVM crashes or shuts down during the process
			WriteAheadLogToken token = new WriteAheadLogToken(this.getNow(), this.incrementalCommitTimestamp);
			this.performWriteAheadLog(token);
		}
		return this.performBranchExclusive(() -> {
			this.commitLock.lock();
			try {

				long time = this.incrementalCommitTimestamp;
				Set<ChangeSetEntry> changeSet = tx.getChangeSet();
				if (changeSet.isEmpty()) {
					// change set is empty -> there is nothing to commit
					return this.incrementalCommitTimestamp;
				}
				Map<String, Map<String, byte[]>> keyspaceToKeyToValue = Maps.newHashMap();
				Map<ChronoIdentifier, Pair<Object, Object>> entriesToIndex = Maps.newHashMap();
				// prepare a transaction to fetch the "old values" with. The old values
				// are the ones that existed BEFORE we started the incremental commit process.
				ChronoDBTransaction oldValueTx = this.tx(tx.getBranchName(), this.getNow());
				for (ChangeSetEntry entry : changeSet) {
					String keyspace = entry.getKeyspace();
					String key = entry.getKey();
					Object oldValue = oldValueTx.get(keyspace, key);
					Object newValue = entry.getValue();
					Set<PutOption> options = entry.getOptions();
					Map<String, byte[]> keyspaceMap = keyspaceToKeyToValue.get(keyspace);
					if (keyspaceMap == null) {
						keyspaceMap = Maps.newHashMap();
						keyspaceToKeyToValue.put(keyspace, keyspaceMap);
					}
					if (entry.isRemove()) {
						keyspaceMap.put(key, null);
					} else {
						byte[] serialForm = this.getOwningDB().getSerializationManager().serialize(newValue);
						keyspaceMap.put(key, serialForm);
					}
					ChronoIdentifier identifier = ChronoIdentifier.create(this.getOwningBranch(), time, keyspace, key);
					if (options.contains(PutOption.NO_INDEX) == false) {
						entriesToIndex.put(identifier, Pair.of(oldValue, newValue));
					}
				}
				try {
					// here, we perform the actual *write* work.
					for (Entry<String, Map<String, byte[]>> entry : keyspaceToKeyToValue.entrySet()) {
						String keyspace = entry.getKey();
						Map<String, byte[]> contents = entry.getValue();
						TemporalDataMatrix matrix = this.getOrCreateMatrix(keyspace, time);
						matrix.put(time, contents);
					}
					IndexManager indexManager = this.getOwningDB().getIndexManager();
					if (indexManager != null) {
						// clear the query cache (if any). The reason for this is that during incremental upates,
						// we can get different results for the same query on the same timestamp. This is due to
						// changes in the same key at the timestamp of the incremental commit process.
						indexManager.clearQueryCache();
						// roll back the changed keys to the state before the incremental commit started
						// NOTE: we need to convert the ChronoIdentifier back to a qualified key (we are NOT interested
						// in the timestamp here, only in keyspace and key)
						Set<QualifiedKey> modifiedKeys = entriesToIndex.keySet().stream()
								.map(id -> QualifiedKey.create(id.getKeyspace(), id.getKey()))
								.collect(Collectors.toSet());
						indexManager.rollback(this.getOwningBranch(), this.getNow(), modifiedKeys);
						// re-index the modified keys
						indexManager.index(entriesToIndex);
					}
					// update the cache (if any)
					if (this.cache != null) {
						this.cache.rollbackToTimestamp(this.getNow());
						this.writeCommitThroughCache(tx, time);
					}
				} catch (Throwable t) {
					// an error occurred, we need to perform the rollback
					this.performRollbackToTimestamp(this.getNow(), keyspaceToKeyToValue.keySet(), true);
					// as a safety measure, we also have to clear the cache
					if (this.cache != null) {
						this.cache.clear();
					}
					// terminate the incremental commit process
					this.terminateIncrementalCommitProcess();
					// after rolling back, we can clear the write ahead log
					this.clearWriteAheadLogToken();
					// throw the commit exception
					throw new ChronoDBCommitException(
							"An error occurred during the commit. Please see root cause for details.", t);
				}
				// note: we do NOT clear the write-ahead log here, because we are still waiting for the terminating
				// full commit.
			} finally {
				// drop the kryo instance we have been using, as it has some internal caches that just consume memory
				KryoManager.destroyKryo();
				this.commitLock.unlock();
			}
			return this.incrementalCommitTimestamp;
		});
	}

	@Override
	public void performIncrementalRollback(final ChronoDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		if (this.isIncrementalCommitProcessOngoing() == false) {
			throw new IllegalStateException("There is no ongoing incremental commit process. Cannot perform rollback.");
		}
		if (this.incrementalCommitTransaction != tx) {
			throw new IllegalArgumentException(
					"Can only rollback an incremental commit on the same transaction that started the incremental commit process.");
		}
		this.performRollbackToTimestamp(this.getNow(), this.getAllKeyspaces(), true);
		// as a safety measure, we also have to clear the cache
		if (this.cache != null) {
			this.cache.clear();
		}
		this.terminateIncrementalCommitProcess();
		// after rolling back, we can clear the write ahead log
		this.clearWriteAheadLogToken();
	}

	@Override
	public Object performGet(final ChronoDBTransaction tx, final QualifiedKey key) {
		return this.performNonExclusive(() -> {
			if (this.cache != null) {
				// first, try to find the result in our cache
				CacheGetResult<Object> cacheGetResult = this.cache.get(tx.getTimestamp(), key);
				if (cacheGetResult.isHit()) {
					// cache hit, return the result immediately
					return cacheGetResult.getValue();
				}
			}
			// need to contact the backing store. 'performRangedGet' automatically caches the result.
			return this.performRangedGet(tx, key).getValue();
		});
	}

	@Override
	public RangedGetResult<Object> performRangedGet(final ChronoDBTransaction tx, final QualifiedKey key) {
		return this.performNonExclusive(() -> {
			String keyspace = key.getKeyspace();
			TemporalDataMatrix matrix = this.getMatrix(keyspace);
			if (matrix == null) {
				if (this.isMasterBranchTKVS()) {
					// matrix doesn't exist, so the get returns null by definition.
					// In case of the ranged get, we return a result with a null value, and an
					// unlimited range.
					return RangedGetResult.createNoValueResult(key, Period.eternal());
				} else {
					// matrix doesn't exist in the child branch, re-route the request to the parent
					ChronoDBTransaction tempTx = this.createOriginBranchTx(tx.getTimestamp());
					return this.getOriginBranchTKVS().performRangedGet(tempTx, key);
				}
			}
			// execute the query on the backend
			RangedGetResult<byte[]> rangedResult = matrix.getRanged(tx.getTimestamp(), key.getKey());
			if (rangedResult.isHit() == false && this.isMasterBranchTKVS() == false) {
				// we did not find anything in our branch; re-route the request and try to find it in the origin branch
				ChronoDBTransaction tempTx = this.createOriginBranchTx(tx.getTimestamp());
				return this.getOriginBranchTKVS().performRangedGet(tempTx, key);
			}
			// we do have a hit in our branch, so let's process it
			byte[] serialForm = rangedResult.getValue();
			Object deserializedValue = null;
			Period range = rangedResult.getRange();
			if (serialForm == null || serialForm.length <= 0) {
				deserializedValue = null;
			} else {
				deserializedValue = this.getOwningDB().getSerializationManager().deserialize(serialForm);
			}
			RangedGetResult<Object> result = RangedGetResult.create(key, deserializedValue, range);
			if (this.cache != null) {
				// cache the result
				this.cache.cache(key, result);
				// depending on the configuration, we may need to duplicate the result before returning it
				if (this.getOwningDB().getConfiguration().isAssumeCachedValuesAreImmutable()) {
					// we may directly return the cached instance, as we can assume it to be immutable
					return result;
				} else {
					// we have to return a duplicate of the cached element, as we cannot assume it to be immutable,
					// and the client may change the returned element. If we did not duplicate it, changes by the
					// client to the returned element would modify our cache state.
					Object duplicatedValue = KryoManager.deepCopy(deserializedValue);
					return RangedGetResult.create(key, duplicatedValue, range);
				}
			}
			return result;
		});
	}

	@Override
	public Set<String> performKeySet(final ChronoDBTransaction tx, final String keyspaceName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		return this.performNonExclusive(() -> {
			Set<String> allKeys = this.getAllKeys(keyspaceName);
			Set<String> existingKeys = allKeys.stream().filter(key -> {
				QualifiedKey qKey = QualifiedKey.create(keyspaceName, key);
				// check if the key exists at the timestamp
				boolean exists = this.performGet(tx, qKey) != null;
				return exists;
			}).collect(Collectors.toSet());
			return existingKeys;
		});
	}

	@Override
	public Iterator<Long> performHistory(final ChronoDBTransaction tx, final QualifiedKey key) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return this.performNonExclusive(() -> {
			TemporalDataMatrix matrix = this.getMatrix(key.getKeyspace());
			if (matrix == null) {
				if (this.isMasterBranchTKVS()) {
					// keyspace doesn't exist, history is empty by definition
					return Collections.emptyIterator();
				} else {
					// re-route the request and ask the parent branch
					ChronoDBTransaction tempTx = this.createOriginBranchTx(tx.getTimestamp());
					return this.getOriginBranchTKVS().performHistory(tempTx, key);
				}
			}
			// the matrix exists in our branch, ask it for the history
			Iterator<Long> iterator = matrix.history(tx.getTimestamp(), key.getKey());
			// concatenate the history in the owning branch (if any)
			if (this.isMasterBranchTKVS() == false) {
				ChronoDBTransaction tempTx = this.createOriginBranchTx(tx.getTimestamp());
				Iterator<Long> parentIterator = this.getOriginBranchTKVS().performHistory(tempTx, key);
				return Iterators.concat(iterator, parentIterator);
			} else {
				// we are the master branch and have no origin branch to ask. Our iterator
				// is therefore the final result.
				return iterator;
			}
		});
	}

	@Override
	public Iterator<TemporalKey> performGetModificationsInKeyspaceBetween(final ChronoDBTransaction tx,
			final String keyspace, final long timestampLowerBound, final long timestampUpperBound) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestampLowerBound >= 0,
				"Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0,
				"Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= tx.getTimestamp(),
				"Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
		checkArgument(timestampUpperBound <= tx.getTimestamp(),
				"Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
		checkArgument(timestampLowerBound <= timestampUpperBound,
				"Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		return this.performNonExclusive(() -> {
			TemporalDataMatrix matrix = this.getMatrix(keyspace);
			if (matrix == null) {
				return Collections.emptyIterator();
			}
			return matrix.getModificationsBetween(timestampLowerBound, timestampUpperBound);
		});
	}

	@Override
	public Object performGetCommitMetadata(final ChronoDBTransaction tx, final long commitTimestamp) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		checkArgument(commitTimestamp <= tx.getTimestamp(),
				"Precondition violation  - argument 'commitTimestamp' must be less than or equal to the transaction timestamp!");
		return this.performNonExclusive(() -> {
			if (this.getOwningBranch().getOrigin() != null
					&& this.getOwningBranch().getBranchingTimestamp() >= commitTimestamp) {
				// ask the parent branch to resolve it
				ChronoDBTransaction originTx = this
						.createOriginBranchTx(this.getOwningBranch().getBranchingTimestamp());
				return this.getOriginBranchTKVS().performGetCommitMetadata(originTx, commitTimestamp);
			}
			return this.getCommitMetadataStore().get(commitTimestamp);
		});
	}

	// =================================================================================================================
	// DUMP METHODS
	// =================================================================================================================

	@Override
	public CloseableIterator<ChronoDBEntry> allEntriesIterator(final long timestamp) {
		return this.performNonExclusive(() -> {
			return new AllEntriesIterator(timestamp);
		});
	}

	@Override
	public void insertEntries(final Set<ChronoDBEntry> entries) {
		this.performBranchExclusive(() -> {
			if (this.cache != null) {
				// insertion of entries can (potentially) completely wreck the consistency of our cache.
				// in order to be safe, we clear it completely.
				this.cache.clear();
			}
			long maxTimestamp = this.getNow();
			SetMultimap<String, UnqualifiedTemporalEntry> keyspaceToEntries = HashMultimap.create();
			for (ChronoDBEntry entry : entries) {
				ChronoIdentifier chronoIdentifier = entry.getIdentifier();
				String keyspace = chronoIdentifier.getKeyspace();
				String key = chronoIdentifier.getKey();
				long timestamp = chronoIdentifier.getTimestamp();
				byte[] value = entry.getValue();
				UnqualifiedTemporalKey unqualifiedKey = new UnqualifiedTemporalKey(key, timestamp);
				UnqualifiedTemporalEntry unqualifiedEntry = new UnqualifiedTemporalEntry(unqualifiedKey, value);
				keyspaceToEntries.put(keyspace, unqualifiedEntry);
				maxTimestamp = Math.max(timestamp, maxTimestamp);
			}
			for (String keyspace : keyspaceToEntries.keySet()) {
				Set<UnqualifiedTemporalEntry> entriesToInsert = keyspaceToEntries.get(keyspace);
				if (entriesToInsert == null || entriesToInsert.isEmpty()) {
					continue;
				}
				long minTimestamp = entriesToInsert.stream().mapToLong(entry -> entry.getKey().getTimestamp()).min()
						.orElse(0L);
				TemporalDataMatrix matrix = this.getOrCreateMatrix(keyspace, minTimestamp);
				matrix.insertEntries(entriesToInsert);
			}
			if (maxTimestamp > this.getNow()) {
				this.setNow(maxTimestamp);
			}
		});
	}

	// =================================================================================================================
	// BRANCH LOCKING
	// =================================================================================================================

	@Override
	public void performNonExclusive(final ChronoNonReturningJob job) {
		// Note that we FIRST acquire the read lock on the database, and THEN the lock on the branch.
		// All locking works "outside in", this way we avoid deadlocks.
		this.getOwningDB().performNonExclusive(() -> {
			this.branchLock.readLock().lock();
			try {
				job.execute();
			} finally {
				this.branchLock.readLock().unlock();
			}
		});
	}

	@Override
	public <T> T performNonExclusive(final ChronoReturningJob<T> job) {
		// Note that we FIRST acquire the read lock on the database, and THEN the lock on the branch.
		// All locking works "outside in", this way we avoid deadlocks.
		return this.getOwningDB().performNonExclusive(() -> {
			this.branchLock.readLock().lock();
			try {
				return job.execute();
			} finally {
				this.branchLock.readLock().unlock();
			}
		});
	}

	@Override
	public void performBranchExclusive(final ChronoNonReturningJob job) {
		// Note that we FIRST acquire the read lock on the database, and THEN the lock on the branch.
		// All locking works "outside in", this way we avoid deadlocks.
		this.getOwningDB().performExclusive(() -> {
			this.branchLock.writeLock().lock();
			try {
				job.execute();
			} finally {
				this.branchLock.writeLock().unlock();
			}
		});
	}

	@Override
	public <T> T performBranchExclusive(final ChronoReturningJob<T> job) {
		// Note that we FIRST acquire the read lock on the database, and THEN the lock on the branch.
		// All locking works "outside in", this way we avoid deadlocks.
		return this.getOwningDB().performExclusive(() -> {
			this.branchLock.writeLock().lock();
			try {
				return job.execute();
			} finally {
				this.branchLock.writeLock().unlock();
			}
		});
	}

	@Override
	public long getNow() {
		long nowInternal = this.getNowInternal();
		return Math.max(this.getOwningBranch().getBranchingTimestamp(), nowInternal);
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	@Override
	protected void verifyTransaction(final ChronoDBTransaction tx) {
		if (tx.getTimestamp() > this.getNow()) {
			throw new InvalidTransactionTimestampException(
					"Transaction timestamp must not be greater than timestamp of last commit!");
		}
		if (this.getOwningDB().getBranchManager().existsBranch(tx.getBranchName()) == false) {
			throw new InvalidTransactionBranchException(
					"The branch '" + tx.getBranchName() + "' does not exist at timestamp '" + tx.getTimestamp() + "'!");
		}
	}

	private void preventBlindOverwrite(final ChronoDBTransaction tx,
			final Map<String, Map<String, byte[]>> keyspaceToKeyToValue) {
		Set<String> keyspaces = this.getKeyspaces(tx);
		for (String keyspace : keyspaceToKeyToValue.keySet()) {
			if (keyspaces.contains(keyspace) == false) {
				// the keyspace is new, so blind overwrite can't happen
				continue;
			}
			// get the matrix representing the keyspace
			TemporalDataMatrix matrix = this.getMatrix(keyspace);
			if (matrix == null) {
				// the keyspace didn't even exist before, so blind overwrite can't happen
				continue;
			}
			Map<String, byte[]> keyToValue = keyspaceToKeyToValue.get(keyspace);
			// check all keys in the key set individually
			for (String key : keyToValue.keySet()) {
				// check when the last commit on that key has occurred
				long lastCommitTimestamp = matrix.lastCommitTimestamp(key);
				// if the last commit timestamp was after our transaction, we have a blind overwrite
				if (lastCommitTimestamp > tx.getTimestamp()) {
					throw new BlindOverwriteException(
							"The key '" + key + "' received a commit since the start of this transaction!");
				}
			}
		}
	}

	/**
	 * Returns the {@link TemporalDataMatrix} responsible for the given keyspace.
	 *
	 * @param keyspace
	 *            The name of the keyspace to get the matrix for. Must not be <code>null</code>.
	 *
	 * @return The temporal data matrix that stores the keyspace data, or <code>null</code> if there is no keyspace for
	 *         the given name.
	 */
	protected TemporalDataMatrix getMatrix(final String keyspace) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		return this.keyspaceToMatrix.get(keyspace);
	}

	/**
	 * Returns the {@link TemporalDataMatrix} responsible for the given keyspace, creating it if it does not exist.
	 *
	 * @param keyspace
	 *            The name of the keyspace to get the matrix for. Must not be <code>null</code>.
	 * @param timestamp
	 *            In case of a "create", this timestamp specifies the creation timestamp of the matrix. Must not be
	 *            negative.
	 *
	 * @return The temporal data matrix that stores the keyspace data. Never <code>null</code>.
	 */
	protected TemporalDataMatrix getOrCreateMatrix(final String keyspace, final long timestamp) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		TemporalDataMatrix matrix = this.getMatrix(keyspace);
		if (matrix == null) {
			matrix = this.createMatrix(keyspace, timestamp);
			this.keyspaceToMatrix.put(keyspace, matrix);
		}
		return matrix;
	}

	protected void writeCommitThroughCache(final ChronoDBTransaction tx, final long timestamp) {
		if (this.cache == null) {
			// no cache to write to; abort
			return;
		}
		// perform the write-through in our cache
		Map<QualifiedKey, Object> keyValues = Maps.newHashMap();
		boolean assumeImmutableValues = this.getOwningDB().getConfiguration().isAssumeCachedValuesAreImmutable();
		for (ChangeSetEntry changeSetEntry : tx.getChangeSet()) {
			String keyspace = changeSetEntry.getKeyspace();
			String key = changeSetEntry.getKey();
			Object value;
			if (changeSetEntry.isRemove()) {
				value = null;
			} else {
				if (assumeImmutableValues) {
					// values are immutable, so we can add the given value to the cache directly
					value = changeSetEntry.getValue();
				} else {
					// values are not immutable, so we add a copy to the cache to prevent modification from outside
					value = KryoManager.deepCopy(changeSetEntry.getValue());
				}
			}
			QualifiedKey qKey = QualifiedKey.create(keyspace, key);
			keyValues.put(qKey, value);
		}
		this.cache.writeThrough(timestamp, keyValues);
	}

	@VisibleForTesting
	public void performRollbackToTimestamp(final long timestamp, final Set<String> modifiedKeyspaces,
			final boolean touchedIndex) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(modifiedKeyspaces, "Precondition violation - argument 'modifiedKeyspaces' must not be NULL!");
		for (String keyspace : modifiedKeyspaces) {
			TemporalDataMatrix matrix = this.getMatrix(keyspace);
			matrix.rollback(timestamp);
		}
		// roll back the commit metadata store
		this.getCommitMetadataStore().rollbackToTimestamp(timestamp);
		// roll back the cache
		if (this.cache != null) {
			this.cache.rollbackToTimestamp(timestamp);
		}
		// only rollback the index manager if we touched it during the commit
		if (touchedIndex) {
			this.getOwningDB().getIndexManager().rollback(this.getOwningBranch(), timestamp);
		}
		this.setNow(timestamp);
	}

	protected void assertThatTransactionMayPerformIncrementalCommit(final ChronoDBTransaction tx) {
		this.incrementalCommitLock.lock();
		try {
			if (this.incrementalCommitTransaction == null) {
				// nobody is currently performing an incremental commit, this means
				// that the given transaction may start an incremental commit process.
				return;
			} else {
				// we have an ongoing incremental commit process. This means that only
				// the transaction that started this process may continue to perform
				// incremental commits, all other transactions are not allowed to
				// commit incrementally before the running process is terminated.
				if (this.incrementalCommitTransaction == tx) {
					// it is the same transaction that started the process,
					// therefore it may continue to perform incremental commits.
					return;
				} else {
					// an incremental commit process is running, but it is controlled
					// by a different transaction. Therefore, this transaction must not
					// perform incremental commits.
					throw new ChronoDBCommitException(
							"An incremental commit process is already being executed by another transaction. "
									+ "Only one incremental commit process may be active at a given time, "
									+ "therefore this incremental commit is rejected.");
				}
			}
		} finally {
			this.incrementalCommitLock.unlock();
		}
	}

	protected void assertThatTransactionMayPerformCommit(final ChronoDBTransaction tx) {
		this.incrementalCommitLock.lock();
		try {
			if (this.incrementalCommitTransaction == null) {
				// no incremental commit is going on; accept the regular commit
				return;
			} else {
				// an incremental commit is occurring. Only accept a full commit from
				// the transaction that started the incremental commit process.
				if (this.incrementalCommitTransaction == tx) {
					// this is the transaction that started the incremental commit process,
					// therefore it may perform the terminating commit
					return;
				} else {
					// an incremental commit process is going on, started by a different
					// transaction. Reject this commit.
					throw new ChronoDBCommitException(
							"An incremental commit process is currently being executed by another transaction. "
									+ "Commits from other transasctions cannot be accepted while an incremental commit process is active, "
									+ "therefore this commit is rejected.");
				}
			}
		} finally {
			this.incrementalCommitLock.unlock();
		}
	}

	protected boolean isFirstIncrementalCommit(final ChronoDBTransaction tx) {
		this.incrementalCommitLock.lock();
		try {
			if (this.incrementalCommitTimestamp > 0) {
				// the incremental commit timestamp has already been decided -> this cannot be the first incremental
				// commit
				return false;
			} else {
				// thie incremental commit timestamp has not yet been decided -> this is the first incremental commit
				return true;
			}
		} finally {
			this.incrementalCommitLock.unlock();
		}
	}

	protected void setUpIncrementalCommit(final ChronoDBTransaction tx) {
		this.incrementalCommitLock.lock();
		try {
			this.incrementalCommitTimestamp = System.currentTimeMillis();
			// make sure we do not write to the same timestamp twice
			while (this.incrementalCommitTimestamp <= this.getNow()) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException ignored) {
				}
				this.incrementalCommitTimestamp = System.currentTimeMillis();
			}
			this.incrementalCommitTransaction = tx;
		} finally {
			this.incrementalCommitLock.unlock();
		}
	}

	protected void terminateIncrementalCommitProcess() {
		this.incrementalCommitLock.lock();
		try {
			this.incrementalCommitTimestamp = -1L;
			this.incrementalCommitTransaction = null;
		} finally {
			this.incrementalCommitLock.unlock();
		}
	}

	protected boolean isIncrementalCommitProcessOngoing() {
		this.incrementalCommitLock.lock();
		try {
			if (this.incrementalCommitTimestamp >= 0) {
				return true;
			} else {
				return false;
			}
		} finally {
			this.incrementalCommitLock.unlock();
		}
	}

	protected boolean isMasterBranchTKVS() {
		return this.owningBranch.getOrigin() == null;
	}

	protected TemporalKeyValueStore getOriginBranchTKVS() {
		return ((BranchInternal) this.owningBranch.getOrigin()).getTemporalKeyValueStore();
	}

	protected ChronoDBTransaction createOriginBranchTx(final long requestedTimestamp) {
		long branchingTimestamp = this.owningBranch.getBranchingTimestamp();
		long timestamp = 0;
		if (requestedTimestamp > branchingTimestamp) {
			// the requested timestamp is AFTER our branching timestamp. Therefore, we must
			// hide any changes in the parent branch that happened after the branching. To
			// do so, we redirect to the branching timestamp.
			timestamp = branchingTimestamp;
		} else {
			// the requested timestamp is BEFORE our branching timestamp. This means that we
			// do not need to mask any changes in our parent branch, and can therefore continue
			// to use the same request timestamp.
			timestamp = requestedTimestamp;
		}

		ChronoDBTransaction tx = this.getOwningDB().txBuilder()
				// switch to the parent branch
				.onBranch(this.owningBranch.getOrigin())
				// use the branching timestamp
				.atTimestamp(timestamp).build();
		return tx;
	}

	// =================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =================================================================================================================

	/**
	 * Returns the internally stored "now" timestamp.
	 *
	 * @return The internally stored "now" timestamp. Never negative.
	 */
	protected abstract long getNowInternal();

	/**
	 * Sets the "now" timestamp on this temporal key value store.
	 *
	 * @param timestamp
	 *            The new timestamp to use as "now". Must not be negative.
	 */
	protected abstract void setNow(long timestamp);

	/**
	 * Creates a new {@link TemporalDataMatrix} for the given keyspace.
	 *
	 * @param keyspace
	 *            The name of the keyspace to create the matrix for. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp at which this keyspace was created. Must not be negative.
	 *
	 * @return The newly created matrix instance for the given keyspace name. Never <code>null</code>.
	 */
	protected abstract TemporalDataMatrix createMatrix(String keyspace, long timestamp);

	/**
	 * Returns the {@link CommitMetadataStore} to work with in this store.
	 *
	 * @return The commit metadata store associated with this store. Never <code>null</code>.
	 */
	protected abstract CommitMetadataStore getCommitMetadataStore();

	/**
	 * Stores the given {@link WriteAheadLogToken} in the persistent store.
	 *
	 * <p>
	 * Non-persistent stores can safely ignore this method.
	 *
	 * @param token
	 *            The token to store. Must not be <code>null</code>.
	 */
	protected abstract void performWriteAheadLog(WriteAheadLogToken token);

	/**
	 * Clears the currently stored {@link WriteAheadLogToken} (if any).
	 *
	 * <p>
	 * If no such token exists, this method does nothing.
	 *
	 * <p>
	 * Non-persistent stores can safely ignore this method.
	 */
	protected abstract void clearWriteAheadLogToken();

	/**
	 * Attempts to return the currently stored {@link WriteAheadLogToken}.
	 *
	 * <p>
	 * If no such token exists, this method returns <code>null</code>.
	 *
	 * <p>
	 * Non-persistent stores can safely ignore this method and should always return <code>null</code>.
	 *
	 * @return The Write Ahead Log Token if it exists, otherwise <code>null</code>.
	 */
	protected abstract WriteAheadLogToken getWriteAheadLogTokenIfExists();

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	private class AllEntriesIterator extends AbstractCloseableIterator<ChronoDBEntry> {

		private final long timestamp;
		private Iterator<String> keyspaceIterator;
		private String currentKeyspace;

		private CloseableIterator<UnqualifiedTemporalEntry> currentEntryIterator;

		public AllEntriesIterator(final long timestamp) {
			AbstractTemporalKeyValueStore self = AbstractTemporalKeyValueStore.this;
			Set<String> keyspaces = Sets.newHashSet(self.getKeyspaces(timestamp));
			this.keyspaceIterator = keyspaces.iterator();
			this.timestamp = System.currentTimeMillis();
		}

		private void tryMoveToNextIterator() {
			if (this.currentEntryIterator != null && this.currentEntryIterator.hasNext()) {
				// current iterator has more elements; stay here
				return;
			}
			// the current iterator is done, close it
			if (this.currentEntryIterator != null) {
				this.currentEntryIterator.close();
			}
			while (true) {
				// move to the next keyspace (if possible)
				if (this.keyspaceIterator.hasNext() == false) {
					// we are at the end of all keyspaces
					this.currentKeyspace = null;
					this.currentEntryIterator = null;
					return;
				}
				// go to the next keyspace
				this.currentKeyspace = this.keyspaceIterator.next();
				// acquire the entry iterator for this keyspace
				TemporalDataMatrix matrix = AbstractTemporalKeyValueStore.this.getMatrix(this.currentKeyspace);
				if (matrix == null) {
					// there is no entry for this keyspace in this store (may happen if we inherited keyspace from
					// parent branch)
					continue;
				}
				this.currentEntryIterator = matrix.allEntriesIterator(this.timestamp);
				if (this.currentEntryIterator.hasNext()) {
					// we found a non-empty iterator, stay here
					return;
				} else {
					// this iterator is empty as well; close it and move to the next iterator
					this.currentEntryIterator.close();
					continue;
				}
			}

		}

		@Override
		protected boolean hasNextInternal() {
			this.tryMoveToNextIterator();
			if (this.currentEntryIterator == null) {
				return false;
			}
			return this.currentEntryIterator.hasNext();
		}

		@Override
		public ChronoDBEntry next() {
			if (this.hasNext() == false) {
				throw new NoSuchElementException();
			}
			// fetch the next entry from our iterator
			UnqualifiedTemporalEntry rawEntry = this.currentEntryIterator.next();
			// convert this entry into a full ChronoDBEntry
			Branch branch = AbstractTemporalKeyValueStore.this.getOwningBranch();
			String keyspaceName = this.currentKeyspace;
			UnqualifiedTemporalKey unqualifiedKey = rawEntry.getKey();
			String actualKey = unqualifiedKey.getKey();
			byte[] actualValue = rawEntry.getValue();
			long entryTimestamp = unqualifiedKey.getTimestamp();
			ChronoIdentifier chronoIdentifier = ChronoIdentifier.create(branch, entryTimestamp, keyspaceName,
					actualKey);
			ChronoDBEntry chronoDBEntry = ChronoDBEntry.create(chronoIdentifier, actualValue);
			return chronoDBEntry;
		}

		@Override
		protected void closeInternal() {
			if (this.currentEntryIterator != null) {
				this.currentEntryIterator.close();
				this.currentEntryIterator = null;
				this.currentKeyspace = null;
				// "burn out" the keyspace iterator
				while (this.keyspaceIterator.hasNext()) {
					this.keyspaceIterator.next();
				}
			}
		}

	}

}
