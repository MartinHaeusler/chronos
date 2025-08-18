package org.chronos.chronodb.internal.impl.engines.base;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchHeadStatistics;
import org.chronos.chronodb.api.ChangeSetEntry;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.CommitMetadataFilter;
import org.chronos.chronodb.api.Dateback;
import org.chronos.chronodb.api.Dateback.KeyspaceValueTransformation;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitMetadataRejectedException;
import org.chronos.chronodb.api.exceptions.DatebackException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.MutableTransactionConfiguration;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.TemporalDataMatrix;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.cache.CacheGetResult;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.index.IndexManagerInternal;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.BranchHeadStatisticsImpl;
import org.chronos.chronodb.internal.impl.DefaultTransactionConfiguration;
import org.chronos.chronodb.internal.impl.conflict.AtomicConflictImpl;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.chronodb.internal.util.KeySetModifications;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.serialization.KryoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractTemporalKeyValueStore extends TemporalKeyValueStoreBase implements TemporalKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(AbstractTemporalKeyValueStore.class);

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

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
     * <p>
     * Also note that (as the name implies) this lock is for commit operations only. Read operations do not need to
     * acquire this lock at all.
     */
    private final Object commitLock = new Object();

    private final BranchInternal owningBranch;
    private final ChronoDBInternal owningDB;
    protected final Map<String, TemporalDataMatrix> keyspaceToMatrix = Maps.newHashMap();

    /**
     * This lock is used to protect incremental commit data from illegal concurrent access.
     */
    protected final Object incrementalCommitLock = new Object();
    /**
     * This field is used to keep track of the transaction that is currently executing an incremental commit.
     */
    protected ChronoDBTransaction incrementalCommitTransaction = null;
    /**
     * This timestamp will be written to during incremental commits, all of them will write to this timestamp.
     */
    protected long incrementalCommitTimestamp = -1L;

    protected Consumer<ChronoDBTransaction> debugCallbackBeforePrimaryIndexUpdate;
    protected Consumer<ChronoDBTransaction> debugCallbackBeforeSecondaryIndexUpdate;
    protected Consumer<ChronoDBTransaction> debugCallbackBeforeMetadataUpdate;
    protected Consumer<ChronoDBTransaction> debugCallbackBeforeCacheUpdate;
    protected Consumer<ChronoDBTransaction> debugCallbackBeforeNowTimestampUpdate;
    protected Consumer<ChronoDBTransaction> debugCallbackBeforeTransactionCommitted;

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
        log.warn("There has been an error during the last shutdown. ChronoDB will attempt to recover to the last consistent state (this may take a few minutes).");
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
        try (AutoLock lock = this.lockNonExclusive()) {
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
        }
    }

    @Override
    public Set<String> getKeyspaces(final ChronoDBTransaction tx) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        return this.getKeyspaces(tx.getTimestamp());
    }

    @Override
    public Set<String> getKeyspaces(final long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        try (AutoLock lock = this.lockNonExclusive()) {
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
        }
    }

    @Override
    public long getNow() {
        try (AutoLock lock = this.lockNonExclusive()) {
            long nowInternal = this.getNowInternal();
            long now = Math.max(this.getOwningBranch().getBranchingTimestamp(), nowInternal);
            // see if we have an open transaction
            WriteAheadLogToken walToken = this.getWriteAheadLogTokenIfExists();
            if (walToken != null) {
                // transaction is open, we must not read after the transaction start
                now = Math.min(now, walToken.getNowTimestampBeforeCommit());
            }
            return now;
        }
    }

    @Override
    public long performCommit(final ChronoDBTransaction tx, final Object commitMetadata) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        this.assertThatTransactionMayPerformCommit(tx);
        boolean performanceLoggingActive = this.owningDB.getConfiguration().isCommitPerformanceLoggingActive();
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
        String perfLogPrefix = "[PERF ChronoDB] Commit (" + tx.getBranchName() + "@" + tx.getTimestamp() + ")";
        long timeBeforeLockAcquisition = System.currentTimeMillis();
        try (AutoLock lock = this.lockBranchExclusive()) {
            synchronized (this.commitLock) {
                try {
                    if (performanceLoggingActive) {
                        log.info(perfLogPrefix + " -> Lock Acquisition: " + (System.currentTimeMillis() - timeBeforeLockAcquisition) + "ms.");
                    }
                    if (this.isIncrementalCommitProcessOngoing() && tx.getChangeSet().isEmpty() == false) {
                        // "terminate" the incremental commit process with a FINAL incremental commit,
                        // then continue with a true commit that has an EMPTY change set
                        tx.commitIncremental();
                    }
                    if (this.isIncrementalCommitProcessOngoing() == false) {
                        if (tx.getChangeSet().isEmpty()) {
                            // change set is empty -> there is nothing to commit
                            return -1;
                        }
                    }
                    long time = 0;
                    if (this.isIncrementalCommitProcessOngoing()) {
                        // use the incremental commit timestamp
                        time = this.incrementalCommitTimestamp;
                    } else {
                        // use the current transaction time
                        time = this.waitForNextValidCommitTimestamp();
                    }

                    long beforeCommitMetadataFilter = System.currentTimeMillis();
                    CommitMetadataFilter filter = this.getOwningDB().getCommitMetadataFilter();
                    if (filter != null) {
                        if (filter.doesAccept(tx.getBranchName(), time, commitMetadata) == false) {
                            String className = (commitMetadata == null ? "NULL" : commitMetadata.getClass().getName());
                            throw new ChronoDBCommitMetadataRejectedException("The given Commit Metadata object (class: " + className + ") was rejected by the commit metadata filter! Cancelling commit.");
                        }
                    }
                    if (performanceLoggingActive) {
                        log.info(perfLogPrefix + " -> Commit Metadata Filter: " + (System.currentTimeMillis() - beforeCommitMetadataFilter) + "ms.");
                    }

                    long beforeChangeSetAnalysis = System.currentTimeMillis();
                    ChangeSet changeSet = this.analyzeChangeSet(tx, tx, time);
                    if (performanceLoggingActive) {
                        log.info(perfLogPrefix + " -> Change Set Analysis: " + (System.currentTimeMillis() - beforeChangeSetAnalysis) + "ms.");
                    }

                    if (this.isIncrementalCommitProcessOngoing() == false) {
                        long beforeWalTokenHandling = System.currentTimeMillis();
                        // check that no WAL token exists on disk
                        this.performRollbackToWALTokenIfExists();
                        // before we begin the writing to disk, we store a token as a file. This token
                        // will allow us to recover on the next startup in the event that the JVM crashes or
                        // is being shut down during the commit process.
                        WriteAheadLogToken token = new WriteAheadLogToken(this.getNow(), time);
                        this.performWriteAheadLog(token);
                        if (performanceLoggingActive) {
                            log.info(perfLogPrefix + " -> WAL Token Handling: " + (System.currentTimeMillis() - beforeWalTokenHandling) + "ms.");
                        }
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
                        this.debugCallbackBeforePrimaryIndexUpdate(tx);
                        long beforePrimaryIndexUpdate = System.currentTimeMillis();
                        this.updatePrimaryIndex(perfLogPrefix + " -> Primary Index Update", time, changeSet);
                        if (performanceLoggingActive) {
                            log.info(perfLogPrefix + " -> Primary Index Update: " + (System.currentTimeMillis() - beforePrimaryIndexUpdate) + "ms.");
                        }
                        this.debugCallbackBeforeSecondaryIndexUpdate(tx);
                        long beforeSecondaryIndexupdate = System.currentTimeMillis();
                        touchedIndex = this.updateSecondaryIndices(changeSet) || touchedIndex;
                        if (performanceLoggingActive) {
                            log.info(perfLogPrefix + " -> Secondary Index Update: " + (System.currentTimeMillis() - beforeSecondaryIndexupdate) + "ms.");
                        }
                        this.debugCallbackBeforeMetadataUpdate(tx);
                        // write the commit metadata object (this will also register the commit, even if no metadata is
                        // given)
                        long beforeCommitMetadataStoring = System.currentTimeMillis();
                        this.getCommitMetadataStore().put(time, commitMetadata);
                        if (performanceLoggingActive) {
                            log.info(perfLogPrefix + " -> Commit Metadata Store: " + (System.currentTimeMillis() - beforeCommitMetadataStoring) + "ms.");
                        }
                        this.debugCallbackBeforeCacheUpdate(tx);
                        // update the cache (if any)
                        long beforeCacheUpdate = System.currentTimeMillis();
                        if (this.getCache() != null && this.isIncrementalCommitProcessOngoing()) {
                            this.getCache().rollbackToTimestamp(this.getNow());
                        }
                        this.writeCommitThroughCache(tx.getBranchName(), time, changeSet.getEntriesByKeyspace());
                        if (performanceLoggingActive) {
                            log.info(perfLogPrefix + " -> Cache Update: " + (System.currentTimeMillis() - beforeCacheUpdate) + "ms.");
                        }
                        this.debugCallbackBeforeNowTimestampUpdate(tx);
                        long beforeSetNow = System.currentTimeMillis();
                        this.setNow(time);
                        if (performanceLoggingActive) {
                            log.info(perfLogPrefix + " -> Set Now: " + (System.currentTimeMillis() - beforeSetNow) + "ms.");
                        }
                        this.debugCallbackBeforeTransactionCommitted(tx);
                    } catch (Throwable t) {
                        // an error occurred, we need to perform the rollback
                        this.rollbackCurrentCommit(changeSet, touchedIndex);
                        // throw the commit exception
                        throw new ChronoDBCommitException(
                            "An error occurred during the commit. Please see root cause for details.", t);
                    }
                    long beforeClearWalToken = System.currentTimeMillis();
                    // everything ok in this commit, we can clear the write ahead log
                    this.clearWriteAheadLogToken();
                    if (performanceLoggingActive) {
                        log.info(perfLogPrefix + " -> Clear WAL token: " + (System.currentTimeMillis() - beforeClearWalToken) + "ms.");
                    }
                    // clear the branch head statistics cache, forcing a recalculation on the next access
                    this.owningDB.getStatisticsManager().clearBranchHeadStatistics(tx.getBranchName());
                    return time;
                } finally {
                    if (this.isIncrementalCommitProcessOngoing()) {
                        this.terminateIncrementalCommitProcess();
                    }
                    long beforeDestroyKryo = System.currentTimeMillis();
                    // drop the kryo instance we have been using, as it has some internal caches that just consume memory
                    KryoManager.destroyKryo();
                    if (performanceLoggingActive) {
                        log.info(perfLogPrefix + " -> Destroy Kryo: " + (System.currentTimeMillis() - beforeDestroyKryo) + "ms.");
                    }
                }
            }
        }
    }

    @Override
    public long performCommitIncremental(final ChronoDBTransaction tx) throws ChronoDBCommitException {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        String perfLogPrefix = "[PERF ChronoDB] Commit (" + tx.getBranchName() + "@" + tx.getTimestamp() + ")";
        try (AutoLock lock = this.lockBranchExclusive()) {
            // make sure that this transaction may start (or continue with) an incremental commit process
            this.assertThatTransactionMayPerformIncrementalCommit(tx);
            // set up the incremental commit process, if this is the first incremental commit
            if (this.isFirstIncrementalCommit(tx)) {
                this.setUpIncrementalCommit(tx);
                this.performRollbackToWALTokenIfExists();
                // store the WAL token. We will need it to recover if the JVM crashes or shuts down during the process
                WriteAheadLogToken token = new WriteAheadLogToken(this.getNow(), this.incrementalCommitTimestamp);
                this.performWriteAheadLog(token);
            }
            synchronized (this.commitLock) {
                try {
                    long time = this.incrementalCommitTimestamp;
                    if (tx.getChangeSet().isEmpty()) {
                        // change set is empty -> there is nothing to commit
                        return this.incrementalCommitTimestamp;
                    }
                    // prepare a transaction to fetch the "old values" with. The old values
                    // are the ones that existed BEFORE we started the incremental commit process.
                    ChronoDBTransaction oldValueTx = this.tx(tx.getBranchName(), this.getNow());
                    ChangeSet changeSet = this.analyzeChangeSet(tx, oldValueTx, time);
                    try {
                        // here, we perform the actual *write* work.
                        this.debugCallbackBeforePrimaryIndexUpdate(tx);
                        this.updatePrimaryIndex(perfLogPrefix + " -> Primary Index Update", time, changeSet);
                        this.debugCallbackBeforeSecondaryIndexUpdate(tx);
                        this.updateSecondaryIndices(changeSet);
                        this.debugCallbackBeforeCacheUpdate(tx);
                        // update the cache (if any)
                        this.getCache().rollbackToTimestamp(this.getNow());
                        this.writeCommitThroughCache(tx.getBranchName(), time, changeSet.getEntriesByKeyspace());
                        this.debugCallbackBeforeTransactionCommitted(tx);
                    } catch (Throwable t) {
                        // an error occurred, we need to perform the rollback
                        this.performRollbackToTimestamp(this.getNow(), changeSet.getEntriesByKeyspace().keySet(), true);
                        // as a safety measure, we also have to clear the cache
                        this.getCache().clear();
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
                }
            }
            return this.incrementalCommitTimestamp;
        }
    }

    private void performRollbackToWALTokenIfExists() {
        // check if a WAL token exists
        WriteAheadLogToken walToken = this.getWriteAheadLogTokenIfExists();
        if (walToken != null) {
            // a write-ahead log token already exists in our store. This means that another transaction
            // failed mid-way.
            log.warn("The transaction log indicates that a transaction after timestamp '"
                + walToken.getNowTimestampBeforeCommit() + "' has failed. Will perform a rollback to timestamp '"
                + walToken.getNowTimestampBeforeCommit() + "'. This may take a while.");
            this.performRollbackToTimestamp(walToken.getNowTimestampBeforeCommit(), this.getAllKeyspaces(), true);
        }
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
        this.getCache().clear();
        this.terminateIncrementalCommitProcess();
        // after rolling back, we can clear the write ahead log
        this.clearWriteAheadLogToken();
    }

    @Override
    public Object performGet(final ChronoDBTransaction tx, final QualifiedKey key) {
        return this.performGet(tx.getBranchName(), key, tx.getTimestamp(), false);
    }

    @Override
    public byte[] performGetBinary(final ChronoDBTransaction tx, final QualifiedKey key) {
        return (byte[]) this.performGet(tx.getBranchName(), key, tx.getTimestamp(), true);
    }

    private Object performGet(final String branchName, final QualifiedKey qKey, final long timestamp, boolean binary) {
        try (AutoLock lock = this.lockNonExclusive()) {
            // binary gets are excluded from caching
            if (!binary) {
                // first, try to find the result in our cache
                CacheGetResult<Object> cacheGetResult = this.getCache().get(branchName, timestamp, qKey);
                if (cacheGetResult.isHit()) {
                    Object result = cacheGetResult.getValue();
                    if (result == null) {
                        return null;
                    }
                    // cache hit, return the result immediately
                    if (this.getOwningDB().getConfiguration().isAssumeCachedValuesAreImmutable()) {
                        // return directly
                        return cacheGetResult.getValue();
                    } else {
                        // return a copy
                        return KryoManager.deepCopy(result);
                    }
                }
            }
            // need to contact the backing store. 'performRangedGet' automatically caches the result.
            GetResult<?> getResult = this.performRangedGetInternal(branchName, qKey, timestamp, binary);
            if (getResult.isHit() == false) {
                return null;
            } else {
                return getResult.getValue();
            }
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public GetResult<Object> performRangedGet(final ChronoDBTransaction tx, final QualifiedKey key) {
        return (GetResult) this.performRangedGetInternal(tx.getBranchName(), key, tx.getTimestamp(), false);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public GetResult<byte[]> performRangedGetBinary(final ChronoDBTransaction tx, final QualifiedKey key) {
        return (GetResult) this.performRangedGetInternal(tx.getBranchName(), key, tx.getTimestamp(), true);
    }

    protected GetResult<?> performRangedGetInternal(final String branchName, final QualifiedKey qKey,
                                                    final long timestamp, final boolean binary) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        checkNotNull(qKey, "Precondition violation - argument 'qKey' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        try (AutoLock lock = this.lockNonExclusive()) {
            TemporalDataMatrix matrix = this.getMatrix(qKey.getKeyspace());
            if (matrix == null) {
                if (this.isMasterBranchTKVS()) {
                    // matrix doesn't exist, so the get returns null by definition.
                    // In case of the ranged get, we return a result with a null value, and an
                    // unlimited range.
                    return GetResult.createNoValueResult(qKey, Period.eternal());
                } else {
                    // matrix doesn't exist in the child branch, re-route the request to the parent
                    ChronoDBTransaction tempTx = this.createOriginBranchTx(timestamp);
                    if (binary) {
                        return this.getOriginBranchTKVS().performRangedGetBinary(tempTx, qKey);
                    } else {
                        return this.getOriginBranchTKVS().performRangedGet(tempTx, qKey);
                    }
                }
            }
            // execute the query on the backend
            GetResult<byte[]> rangedResult = matrix.get(timestamp, qKey.getKey());
            if (rangedResult.isHit() == false && this.isMasterBranchTKVS() == false) {
                // we did not find anything in our branch; re-route the request and try to find it in the origin branch
                ChronoDBTransaction tempTx = this.createOriginBranchTx(timestamp);
                if (binary) {
                    return this.getOriginBranchTKVS().performRangedGetBinary(tempTx, qKey);
                } else {
                    return this.getOriginBranchTKVS().performRangedGet(tempTx, qKey);
                }
            }
            // we do have a hit in our branch, so let's process it
            if (binary) {
                // we want the raw binary result, no need for deserialization or caching
                return rangedResult;
            }
            byte[] serialForm = rangedResult.getValue();
            Object deserializedValue = null;
            Period range = rangedResult.getPeriod();
            if (serialForm == null || serialForm.length <= 0) {
                deserializedValue = null;
            } else {
                deserializedValue = this.getOwningDB().getSerializationManager().deserialize(serialForm);
            }
            GetResult<Object> result = rangedResult.isHit()
                ? GetResult.create(qKey, deserializedValue, range)
                : GetResult.createNoValueResult(qKey, range);
            // cache the result
            this.getCache().cache(branchName, result);
            // depending on the configuration, we may need to duplicate the result before returning it
            if (this.getOwningDB().getConfiguration().isAssumeCachedValuesAreImmutable() || !result.isHit()) {
                // we may directly return the cached instance, as we can assume it to be immutable
                return result;
            } else {
                // we have to return a duplicate of the cached element, as we cannot assume it to be immutable,
                // and the client may change the returned element. If we did not duplicate it, changes by the
                // client to the returned element would modify our cache state.
                Object duplicatedValue = KryoManager.deepCopy(deserializedValue);
                return GetResult.create(qKey, duplicatedValue, range);
            }
        }
    }

    @Override
    public Set<String> performKeySet(final ChronoDBTransaction tx, final String keyspaceName) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
        return this.performKeySet(tx.getBranchName(), tx.getTimestamp(), keyspaceName);
    }

    public Set<String> performKeySet(final String branch, final long timestamp, final String keyspaceName) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            TemporalDataMatrix matrix = this.getMatrix(keyspaceName);
            if (this.getOwningBranch().getOrigin() == null) {
                // we are master, directly apply changes
                if (matrix == null) {
                    // keyspace is not present, return the empty set
                    return Sets.newHashSet();
                }
                KeySetModifications modifications = matrix.keySetModifications(timestamp);
                return Sets.newHashSet(modifications.getAdditions());
            } else {
                // we are a sub-branch, accumulate changes along the way
                Branch origin = this.getOwningBranch().getOrigin();
                long branchingTimestamp = this.getOwningBranch().getBranchingTimestamp();
                long parentTimestamp;
                if (timestamp < branchingTimestamp) {
                    parentTimestamp = timestamp;
                } else {
                    parentTimestamp = branchingTimestamp;
                }
                ChronoDBTransaction tmpTX = this.getOwningDB().tx(origin.getName(), parentTimestamp);
                Set<String> keySet = tmpTX.keySet(keyspaceName);
                if (matrix == null) {
                    // the matrix does not exist in this branch, i.e. nothing was added to it yet,
                    // therefore the accumulated changes from our origins are complete
                    return keySet;
                } else {
                    // add our branch-local modifications
                    KeySetModifications modifications = matrix.keySetModifications(timestamp);
                    modifications.apply(keySet);
                }
                return keySet;
            }
        }
    }

    @Override
    public Iterator<Long> performHistory(final ChronoDBTransaction tx, final QualifiedKey key, final long lowerBound, final long upperBound, final Order order) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(lowerBound >= 0, "Precondition violation - argument 'lowerBound' must not be negative!");
        checkArgument(upperBound >= 0, "Precondition violation - argument 'upperBound' must not be negative!");
        checkArgument(upperBound >= lowerBound, "Precondition violation - argument 'upperBound' must be greater than or equal to argument 'lowerBound'!");
        checkArgument(lowerBound <= tx.getTimestamp(), "Precondition violation - argument 'lowerBound' must be less than or equal to the transaction timestamp!");
        checkArgument(upperBound <= tx.getTimestamp(), "Precondition violation - argument 'upperBound' must be less than or equal to the transaction timestamp!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            TemporalDataMatrix matrix = this.getMatrix(key.getKeyspace());
            if (matrix == null) {
                if (this.isMasterBranchTKVS()) {
                    // keyspace doesn't exist, history is empty by definition
                    return Collections.emptyIterator();
                } else {
                    // re-route the request and ask the parent branch
                    ChronoDBTransaction tempTx = this.createOriginBranchTx(tx.getTimestamp());
                    //clamp the request range
                    long newUpperBound = Math.min(tempTx.getTimestamp(), upperBound);
                    long newLowerBound = Math.min(lowerBound, newUpperBound);
                    return this.getOriginBranchTKVS().performHistory(tempTx, key, newLowerBound, newUpperBound, order);
                }
            }
            // the matrix exists in our branch, ask it for the history
            Iterator<Long> iterator = matrix.history(key.getKey(), lowerBound, upperBound, order);
            if (this.isMasterBranchTKVS()) {
                // we're done here
                return iterator;
            } else {
                // check if the entire range is within the range of this branch
                if (this.owningBranch.getBranchingTimestamp() < lowerBound) {
                    // no need to ask our parent branch, we're done
                    return iterator;
                }
                // ask our parent branch
                ChronoDBTransaction tempTx = this.createOriginBranchTx(tx.getTimestamp());
                //clamp the request range
                long newUpperBound = Math.min(tempTx.getTimestamp(), upperBound);
                long newLowerBound = Math.min(lowerBound, newUpperBound);
                Iterator<Long> parentBranchIterator = this.getOriginBranchTKVS().performHistory(tempTx, key, newLowerBound, newUpperBound, order);
                switch (order) {
                    case ASCENDING:
                        // if we have ASCENDING order, we need to use our parent iterator first, then our own iterator
                        return Iterators.concat(parentBranchIterator, iterator);
                    case DESCENDING:
                        // if we have DESCENDING order, we need to use our iterator first, then the parent iterator
                        return Iterators.concat(iterator, parentBranchIterator);
                    default:
                        throw new UnknownEnumLiteralException(order);
                }
            }
        }
    }

    @Override
    public long performGetLastModificationTimestamp(final ChronoDBTransaction tx, final QualifiedKey key) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            TemporalDataMatrix matrix = this.getMatrix(key.getKeyspace());
            long lastCommitTimestamp = -1;
            if (matrix != null) {
                lastCommitTimestamp = matrix.lastCommitTimestamp(key.getKey(), tx.getTimestamp());
            }
            if (lastCommitTimestamp < 0) {
                // there was no commit in OUR branch, but maybe in the parent?
                if (this.isMasterBranchTKVS()) {
                    // keyspace doesn't exist, history is empty by definition
                    return -1;
                } else {
                    // re-route the request and ask the parent branch
                    long newTransactionTimestamp = Math.min(tx.getTimestamp(), this.owningBranch.getBranchingTimestamp());
                    ChronoDBTransaction tempTx = this.createOriginBranchTx(newTransactionTimestamp);
                    return this.getOriginBranchTKVS().performGetLastModificationTimestamp(tempTx, key);
                }
            }
            return lastCommitTimestamp;
        }
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
        try (AutoLock lock = this.lockNonExclusive()) {
            TemporalDataMatrix matrix = this.getMatrix(keyspace);
            if (matrix == null) {
                if (this.isMasterBranchTKVS()) {
                    // keyspace doesn't exist, history is empty by definition
                    return Collections.emptyIterator();
                } else {
                    // re-route the request and ask the parent branch
                    ChronoDBTransaction tempTx = this.createOriginBranchTx(tx.getTimestamp());
                    //clamp the request range
                    long newUpperBound = Math.min(tempTx.getTimestamp(), timestampUpperBound);
                    long newLowerBound = Math.min(timestampLowerBound, newUpperBound);
                    return this.getOriginBranchTKVS().performGetModificationsInKeyspaceBetween(tempTx, keyspace, newLowerBound, newUpperBound);
                }
            }
            // the matrix exists in our branch, ask it for the history
            Iterator<TemporalKey> iterator = matrix.getModificationsBetween(timestampLowerBound, timestampUpperBound);
            if (this.isMasterBranchTKVS()) {
                // we're done here
                return iterator;
            } else {
                // check if the entire range is within the range of this branch
                if (this.owningBranch.getBranchingTimestamp() < timestampLowerBound) {
                    // no need to ask our parent branch, we're done
                    return iterator;
                }
                // ask our parent branch
                ChronoDBTransaction tempTx = this.createOriginBranchTx(tx.getTimestamp());
                //clamp the request range
                long newUpperBound = Math.min(tempTx.getTimestamp(), timestampUpperBound);
                long newLowerBound = Math.min(timestampLowerBound, newUpperBound);
                Iterator<TemporalKey> parentBranchIterator = this.getOriginBranchTKVS().performGetModificationsInKeyspaceBetween(tempTx, keyspace, newLowerBound, newUpperBound);
                return Iterators.concat(iterator, parentBranchIterator);
            }
        }
    }

    @Override
    public Iterator<Long> performGetCommitTimestampsBetween(final ChronoDBTransaction tx, final long from,
                                                            final long to, final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(from <= tx.getTimestamp(),
            "Precondition violation - argument 'from' must not be larger than the transaction timestamp!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkArgument(to <= tx.getTimestamp(),
            "Precondition violation - argument 'to' must not be larger than the transaction timestamp!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.getCommitMetadataStore().getCommitTimestampsBetween(from, to, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Entry<Long, Object>> performGetCommitMetadataBetween(final ChronoDBTransaction tx, final long from,
                                                                         final long to, final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(from <= tx.getTimestamp(),
            "Precondition violation - argument 'from' must not be larger than the transaction timestamp!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkArgument(to <= tx.getTimestamp(),
            "Precondition violation - argument 'to' must not be larger than the transaction timestamp!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitMetadataBetween(from, to, order, includeSystemInternalCommits);
        }
    }

    @Override
    public Iterator<Long> performGetCommitTimestampsPaged(final ChronoDBTransaction tx, final long minTimestamp,
                                                          final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(minTimestamp <= tx.getTimestamp(),
            "Precondition violation - argument 'minTimestamp' must not be larger than the transaction timestamp!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(maxTimestamp <= tx.getTimestamp(),
            "Precondition violation - argument 'maxTimestamp' must not be larger than the transaction timestamp!");
        checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
        checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitTimestampsPaged(minTimestamp, maxTimestamp, pageSize,
                pageIndex, order, includeSystemInternalCommits);
        }
    }

    @Override
    public Iterator<Entry<Long, Object>> performGetCommitMetadataPaged(final ChronoDBTransaction tx,
                                                                       final long minTimestamp, final long maxTimestamp, final int pageSize, final int pageIndex,
                                                                       final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(minTimestamp <= tx.getTimestamp(),
            "Precondition violation - argument 'minTimestamp' must not be larger than the transaction timestamp!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(maxTimestamp <= tx.getTimestamp(),
            "Precondition violation - argument 'maxTimestamp' must not be larger than the transaction timestamp!");
        checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
        checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitMetadataPaged(minTimestamp, maxTimestamp, pageSize, pageIndex,
                order, includeSystemInternalCommits);
        }
    }

    @Override
    public List<Entry<Long, Object>> performGetCommitMetadataAround(final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitMetadataAround(timestamp, count, includeSystemInternalCommits);
        }
    }

    @Override
    public List<Entry<Long, Object>> performGetCommitMetadataBefore(final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitMetadataBefore(timestamp, count, includeSystemInternalCommits);
        }
    }

    @Override
    public List<Entry<Long, Object>> performGetCommitMetadataAfter(final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitMetadataAfter(timestamp, count, includeSystemInternalCommits);
        }
    }

    @Override
    public List<Long> performGetCommitTimestampsAround(final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitTimestampsAround(timestamp, count, includeSystemInternalCommits);
        }
    }

    @Override
    public List<Long> performGetCommitTimestampsBefore(final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitTimestampsBefore(timestamp, count, includeSystemInternalCommits);
        }
    }

    @Override
    public List<Long> performGetCommitTimestampsAfter(final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().getCommitTimestampsAfter(timestamp, count, includeSystemInternalCommits);
        }
    }

    @Override
    public int performCountCommitTimestampsBetween(final ChronoDBTransaction tx, final long from, final long to, final boolean includeSystemInternalCommits) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(from <= tx.getTimestamp(),
            "Precondition violation - argument 'from' must not be larger than the transaction timestamp!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkArgument(to <= tx.getTimestamp(),
            "Precondition violation - argument 'to' must not be larger than the transaction timestamp!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().countCommitTimestampsBetween(from, to, includeSystemInternalCommits);
        }
    }

    @Override
    public int performCountCommitTimestamps(final ChronoDBTransaction tx, final boolean includeSystemInternalCommits) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return this.getCommitMetadataStore().countCommitTimestamps(includeSystemInternalCommits);
        }
    }

    @Override
    public Object performGetCommitMetadata(final ChronoDBTransaction tx, final long commitTimestamp) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkArgument(commitTimestamp <= tx.getTimestamp(),
            "Precondition violation  - argument 'commitTimestamp' must be less than or equal to the transaction timestamp!");
        try (AutoLock lock = this.lockNonExclusive()) {
            if (this.getOwningBranch().getOrigin() != null
                && this.getOwningBranch().getBranchingTimestamp() >= commitTimestamp) {
                // ask the parent branch to resolve it
                ChronoDBTransaction originTx = this
                    .createOriginBranchTx(this.getOwningBranch().getBranchingTimestamp());
                return this.getOriginBranchTKVS().performGetCommitMetadata(originTx, commitTimestamp);
            }
            return this.getCommitMetadataStore().get(commitTimestamp);
        }
    }

    @Override
    public Iterator<String> performGetChangedKeysAtCommit(final ChronoDBTransaction tx, final long commitTimestamp,
                                                          final String keyspace) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkArgument(commitTimestamp <= tx.getTimestamp(),
            "Precondition violation - argument 'commitTimestamp' must not be larger than the transaction timestamp!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            if (this.getOwningBranch().getOrigin() != null
                && this.getOwningBranch().getBranchingTimestamp() >= commitTimestamp) {
                // ask the parent branch to resolve it
                ChronoDBTransaction originTx = this
                    .createOriginBranchTx(this.getOwningBranch().getBranchingTimestamp());
                return this.getOriginBranchTKVS().performGetChangedKeysAtCommit(originTx, commitTimestamp, keyspace);
            }
            if (this.getKeyspaces(commitTimestamp).contains(keyspace) == false) {
                return Collections.emptyIterator();
            }
            return this.getMatrix(keyspace).getChangedKeysAtCommit(commitTimestamp);
        }
    }

    // =================================================================================================================
    // DATEBACK METHODS
    // =================================================================================================================

    @Override
    public int datebackPurgeEntries(final Set<TemporalKey> keys) {
        checkNotNull(keys, "Precondition violation - argument 'keys' must not be NULL!");
        if (keys.isEmpty()) {
            return 0;
        }
        ListMultimap<String, TemporalKey> keyspaceToKeys = Multimaps.index(keys, TemporalKey::getKeyspace);
        int successfulPurges = 0;
        try (AutoLock lock = this.owningDB.lockExclusive()) {
            for (Entry<String, Collection<TemporalKey>> keyspaceToEntries : keyspaceToKeys.asMap().entrySet()) {
                String keyspace = keyspaceToEntries.getKey();
                Collection<TemporalKey> entries = keyspaceToEntries.getValue();
                TemporalDataMatrix matrix = this.getMatrix(keyspace);
                if (matrix == null) {
                    // matrix doesn't exist, so entry can't exist => we're done
                    continue;
                }
                Set<UnqualifiedTemporalKey> utks = entries.stream()
                    .map(e -> UnqualifiedTemporalKey.create(e.getKey(), e.getTimestamp()))
                    .collect(Collectors.toSet());
                successfulPurges += matrix.purgeEntries(utks);
                // TODO in theory, we could implement cascading here:
                // - if the last entry of a commit was removed, the commit as a whole could be removed.
                // - if the last entry of a keyset was removed, the keyset could be removed (or at least it's creation could
                // be moved forward in time until the first entry is inserted)
            }
        }
        return successfulPurges;
    }

    @Override
    public Set<TemporalKey> datebackPurgeKey(final String keyspace, final String key) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        try (AutoLock lock = this.owningDB.lockExclusive()) {
            TemporalDataMatrix matrix = this.getMatrix(keyspace);
            if (matrix == null) {
                // matrix doesn't exist, so entry can't exist => we're done
                return Collections.emptySet();
            }
            Collection<Long> affectedTimestamps = matrix.purgeKey(key);
            if (affectedTimestamps.isEmpty()) {
                return Collections.emptySet();
            } else {
                // map the timestamps to temporal keys by using the fixed keyspace and key from our parameters
                return affectedTimestamps.stream().map(ts -> TemporalKey.create(ts, keyspace, key))
                    .collect(Collectors.toSet());
            }
        }
    }

    @Override
    public Set<TemporalKey> datebackPurgeKey(final String keyspace, final String key,
                                             final BiPredicate<Long, Object> predicate) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(predicate, "Precondition violation - argument 'predicate' must not be NULL!");
        Set<TemporalKey> affectedKeys = Sets.newHashSet();
        try (AutoLock lock = this.owningDB.lockExclusive()) {
            TemporalDataMatrix matrix = this.getMatrix(keyspace);
            if (matrix == null) {
                // matrix doesn't exist, so entry can't exist => we're done
                return Collections.emptySet();
            }
            long now = this.getNow();
            List<Long> history = Lists.newArrayList(matrix.history(key, 0, now, Order.DESCENDING));
            for (long timestamp : history) {
                GetResult<byte[]> getResult = matrix.get(timestamp, key);
                byte[] serialValue = getResult.getValue();
                Object deserializedObject = this.deserialize(serialValue);
                boolean match = predicate.test(timestamp, deserializedObject);
                if (match) {
                    boolean purged = this.datebackPurgeEntry(keyspace, key, timestamp);
                    if (purged) {
                        affectedKeys.add(TemporalKey.create(timestamp, keyspace, key));
                    }
                }
            }
        }
        return affectedKeys;
    }

    @Override
    public Set<TemporalKey> datebackPurgeKey(final String keyspace, final String key, final long purgeRangeStart,
                                             final long purgeRangeEnd) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(purgeRangeStart >= 0,
            "Precondition violation - argument 'purgeRangeStart' must not be negative!");
        checkArgument(purgeRangeEnd >= purgeRangeStart,
            "Precondition violation - argument 'purgeRangeEnd' must be greater than or equal to 'purgeRangeStart'!");
        // TODO PERFORMANCE: this could be optimized, as the called method unnecessarily deserializes the value
        BiPredicate<Long, Object> predicate = new BiPredicate<Long, Object>() {
            @Override
            public boolean test(final Long t, final Object u) {
                return purgeRangeStart <= t && t <= purgeRangeEnd;
            }
        };
        return this.datebackPurgeKey(keyspace, key, predicate);
    }

    @Override
    public Set<TemporalKey> datebackPurgeKeyspace(final String keyspace, final long purgeRangeStart, final long purgeRangeEnd) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkArgument(purgeRangeStart >= 0,
            "Precondition violation - argument 'purgeRangeStart' must not be negative!");
        checkArgument(purgeRangeEnd >= purgeRangeStart,
            "Precondition violation - argument 'purgeRangeEnd' must be greater than or equal to 'purgeRangeStart'!");
        try (AutoLock lock = this.owningDB.lockExclusive()) {
            TemporalDataMatrix matrix = this.getMatrix(keyspace);
            if (matrix == null) {
                // matrix doesn't exist, so entry can't exist => we're done
                return Collections.emptySet();
            }
            return matrix.purgeAllEntriesInTimeRange(purgeRangeStart, purgeRangeEnd).stream()
                .map(utk -> utk.toTemporalKey(keyspace))
                .collect(Collectors.toSet());
        }
    }

    @Override
    public Set<TemporalKey> datebackPurgeCommit(final long commitTimestamp) {
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        try (AutoLock lock = this.owningDB.lockExclusive()) {
            Set<String> keyspaces = this.getKeyspaces(commitTimestamp);
            Set<TemporalKey> keysToPurge = Sets.newHashSet();
            for (String keyspace : keyspaces) {
                Iterator<String> keysAtCommit = this.getMatrix(keyspace).getChangedKeysAtCommit(commitTimestamp);
                keysAtCommit.forEachRemaining(key -> {
                    keysToPurge.add(TemporalKey.create(commitTimestamp, keyspace, key));
                });
            }
            this.datebackPurgeEntries(keysToPurge);
            this.getCommitMetadataStore().purge(commitTimestamp);
            return keysToPurge;
        }
    }

    @Override
    public Set<TemporalKey> datebackPurgeCommits(final long purgeRangeStart, final long purgeRangeEnd) {
        checkArgument(purgeRangeStart >= 0,
            "Precondition violation - argument 'purgeRangeStart' must not be negative!");
        checkArgument(purgeRangeEnd >= purgeRangeStart,
            "Precondition violation - argument 'purgeRangeEnd' must be greater than or equal to 'purgeRangeStart'!");
        Set<TemporalKey> affectedKeys = Sets.newHashSet();
        try (AutoLock lock = this.owningDB.lockExclusive()) {
            List<Long> commitTimestamps = Lists.newArrayList(this.getCommitMetadataStore()
                .getCommitTimestampsBetween(purgeRangeStart, purgeRangeEnd, Order.ASCENDING, true));
            for (long commitTimestamp : commitTimestamps) {
                affectedKeys.addAll(this.datebackPurgeCommit(commitTimestamp));
            }
        }
        return affectedKeys;
    }

    @Override
    public Set<TemporalKey> datebackInject(final String keyspace, final String key, final long timestamp,
                                           final Object value) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        Map<QualifiedKey, Object> entries = Maps.newHashMap();
        entries.put(QualifiedKey.create(keyspace, key), value);
        return this.datebackInject(timestamp, entries, null, false);
    }

    @Override
    public Set<TemporalKey> datebackInject(final String keyspace, final String key, final long timestamp,
                                           final Object value, final Object commitMetadata) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.datebackInject(keyspace, key, timestamp, value, commitMetadata, false);
    }

    @Override
    public Set<TemporalKey> datebackInject(final String keyspace, final String key, final long timestamp,
                                           final Object value, final Object commitMetadata, final boolean overrideCommitMetadata) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        Map<QualifiedKey, Object> entries = Maps.newHashMap();
        entries.put(QualifiedKey.create(keyspace, key), value);
        return this.datebackInject(timestamp, entries, commitMetadata, overrideCommitMetadata);
    }

    @Override
    public Set<TemporalKey> datebackInject(final long timestamp, final Map<QualifiedKey, Object> entries) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
        return this.datebackInject(timestamp, entries, null, false);
    }

    @Override
    public Set<TemporalKey> datebackInject(final long timestamp, final Map<QualifiedKey, Object> entries,
                                           final Object commitMetadata) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
        return this.datebackInject(timestamp, entries, commitMetadata, false);
    }

    @Override
    public Set<TemporalKey> datebackInject(final long timestamp, final Map<QualifiedKey, Object> entries,
                                           final Object commitMetadata, final boolean overrideCommitMetadata) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
        Set<ChronoDBEntry> dbEntries = Sets.newHashSet();
        String branch = this.getOwningBranch().getName();
        for (Entry<QualifiedKey, Object> entry : entries.entrySet()) {
            QualifiedKey qualifiedKey = entry.getKey();
            Object value = entry.getValue();
            byte[] serialized = this.serialize(value);
            ChronoIdentifier chronoIdentifier = ChronoIdentifier.create(branch, timestamp, qualifiedKey);
            ChronoDBEntry dbEntry = ChronoDBEntry.create(chronoIdentifier, serialized);
            dbEntries.add(dbEntry);
        }
        this.insertEntries(dbEntries, true);
        // check if we either don't have a commit at this timestamp in the commit log, or we should override the
        // metadata
        if (this.getCommitMetadataStore().hasCommitAt(timestamp) == false || overrideCommitMetadata) {
            this.getCommitMetadataStore().put(timestamp, commitMetadata);
        }
        // we (defensively) assume here that all db entries have indeed been changed
        Set<TemporalKey> changedKeys = dbEntries.stream().map(e -> e.getIdentifier().toTemporalKey())
            .collect(Collectors.toSet());
        return changedKeys;
    }

    @Override
    public Set<TemporalKey> datebackTransformEntry(final String keyspace, final String key, final long timestamp,
                                                   final Function<Object, Object> transformation) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(transformation, "Precondition violation - argument 'transformation' must not be NULL!");
        TemporalDataMatrix matrix = this.getMatrix(keyspace);
        if (matrix == null) {
            // matrix doesn't exist -> we are done
            return Collections.emptySet();
        }
        UnqualifiedTemporalEntry entry = this.readAndTransformSingleEntryInMemory(keyspace, key, timestamp,
            transformation);
        if (entry == null) {
            // transformation function stated that the entry is unchanged
            return Collections.emptySet();
        }
        matrix.insertEntries(Collections.singleton(entry), true);
        return Collections.singleton(TemporalKey.create(timestamp, keyspace, key));
    }

    public Set<TemporalKey> datebackTransformEntries(final Set<TemporalKey> keys, final BiFunction<TemporalKey, Object, Object> valueTransformation) {
        checkNotNull(keys, "Precondition violation - argument 'keys' must not be NULL!");
        checkNotNull(valueTransformation, "Precondition violation - argument 'valueTransformation' must not be NULL!");
        if (keys.isEmpty()) {
            return Collections.emptySet();
        }
        ListMultimap<String, TemporalKey> keyspaceToKeys = Multimaps.index(keys, key -> key.getKeyspace());
        Set<TemporalKey> resultSet = Sets.newHashSet();
        for (Entry<String, Collection<TemporalKey>> keyspaceToKeysEntry : keyspaceToKeys.asMap().entrySet()) {
            String keyspace = keyspaceToKeysEntry.getKey();
            Collection<TemporalKey> localKeys = keyspaceToKeysEntry.getValue();
            TemporalDataMatrix matrix = this.getMatrix(keyspace);
            if (matrix == null) {
                // matrix doesn't exist -> we are done in this keyspace
                continue;
            }
            Set<UnqualifiedTemporalEntry> transformedEntries = localKeys.stream().map(localKey ->
                this.readAndTransformSingleEntryInMemory(
                    keyspace,
                    localKey.getKey(),
                    localKey.getTimestamp(),
                    (oldValue) -> valueTransformation.apply(localKey, oldValue)
                )
            ).filter(java.util.Objects::nonNull).collect(Collectors.toSet());
            matrix.insertEntries(transformedEntries, true);
            resultSet.addAll(transformedEntries.stream().map(UnqualifiedTemporalEntry::getKey).map(utk -> utk.toTemporalKey(keyspace)).collect(Collectors.toSet()));
        }
        return resultSet;
    }

    @Override
    public Set<TemporalKey> datebackTransformValuesOfKey(final String keyspace, final String key,
                                                         final BiFunction<Long, Object, Object> transformation) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(transformation, "Precondition violation - argument 'transformation' must not be NULL!");
        TemporalDataMatrix matrix = this.getMatrix(keyspace);
        if (matrix == null) {
            // matrix doesn't exist -> we are done
            return Collections.emptySet();
        }
        int maxBatchSize = 1000; // we batch this operation internally in order to prevent out-of-memory issues
        Set<UnqualifiedTemporalEntry> newEntries = Sets.newHashSet();
        Set<TemporalKey> modifiedKeys = Sets.newHashSet();
        Iterator<Long> localHistory = matrix.history(key, 0, this.getNow(), Order.DESCENDING);
        while (localHistory.hasNext()) {
            long timestamp = localHistory.next();
            UnqualifiedTemporalEntry entry = this.readAndTransformSingleEntryInMemory(keyspace, key, timestamp,
                (oldVal) -> transformation.apply(timestamp, oldVal));
            if (entry == null) {
                // transformation stated that the entry is unchanged
                continue;
            }
            newEntries.add(entry);
            if (newEntries.size() >= maxBatchSize) {
                // flush this batch of entries back into the matrix
                matrix.insertEntries(newEntries, true);
                modifiedKeys.addAll(
                    newEntries.stream().map(e -> TemporalKey.create(e.getKey().getTimestamp(), keyspace, key))
                        .collect(Collectors.toSet()));
                newEntries.clear();
            }
        }
        // flush the entries which remain in our batch
        if (newEntries.isEmpty() == false) {
            matrix.insertEntries(newEntries, true);
            modifiedKeys
                .addAll(newEntries.stream().map(e -> TemporalKey.create(e.getKey().getTimestamp(), keyspace, key))
                    .collect(Collectors.toSet()));
        }
        return modifiedKeys;
    }

    @Override
    public Set<TemporalKey> datebackTransformCommit(final long commitTimestamp,
                                                    final Function<Map<QualifiedKey, Object>, Map<QualifiedKey, Object>> transformation) {
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(transformation, "Precondition violation - argument 'transformation' must not be NULL!");
        // fetch the data contained in the commit
        Set<String> keyspaces = this.getKeyspaces(commitTimestamp);
        Map<QualifiedKey, Object> commitContent = Maps.newHashMap();
        for (String keyspace : keyspaces) {
            TemporalDataMatrix matrix = this.getMatrix(keyspace);
            if (matrix == null) {
                // we have no matrix, so we certainly have no commit
                continue;
            }
            Iterator<String> changedKeysAtCommit = matrix.getChangedKeysAtCommit(commitTimestamp);
            changedKeysAtCommit.forEachRemaining(key -> {
                GetResult<byte[]> getResult = this.readEntryAtCoordinates(keyspace, key, commitTimestamp);
                if (getResult == null) {
                    throw new IllegalStateException("Failed to read entry of commit!");
                }
                Object value = this.deserialize(getResult.getValue());
                commitContent.put(QualifiedKey.create(keyspace, key), value);
            });
        }
        // apply the transformation in-memory
        Map<QualifiedKey, Object> transformedMap = transformation.apply(Collections.unmodifiableMap(commitContent));
        // check if we lost some entries
        Set<TemporalKey> removedKeys = commitContent.keySet().stream()
            .filter(key -> transformedMap.containsKey(key) == false)
            .map(key -> TemporalKey.create(commitTimestamp, key.getKeyspace(), key.getKey()))
            .collect(Collectors.toSet());
        this.datebackPurgeEntries(removedKeys);
        // inject the entries which are new
        Map<QualifiedKey, Object> addedEntries = Maps.filterEntries(transformedMap,
            entry -> commitContent.containsKey(entry.getKey()) == false);
        this.datebackInject(commitTimestamp, addedEntries);
        // transform the values of the remaining entries
        Map<TemporalKey, Object> temporalKeyToNewValue = Maps.newHashMap();

        transformedMap.entrySet().stream()
            // consider only the entries which are contained in the transformed commit
            .filter(e -> commitContent.containsKey(e.getKey()))
            // ignore entries which are flagged as unchanged
            .filter(e -> e.getValue() != Dateback.UNCHANGED)
            // construct temporal keys from the qualified keys to ensure the lower-level API matches
            .forEach(e -> {
                QualifiedKey qKey = e.getKey();
                TemporalKey tKey = TemporalKey.create(commitTimestamp, qKey.getKeyspace(), qKey.getKey());
                temporalKeyToNewValue.put(tKey, e.getValue());
            });

        this.datebackTransformEntries(temporalKeyToNewValue.keySet(), (key, oldValue) -> temporalKeyToNewValue.get(key));

        Set<TemporalKey> changedTemporalKeys = Sets.newHashSet();
        changedTemporalKeys.addAll(removedKeys);
        for (QualifiedKey addedKey : addedEntries.keySet()) {
            changedTemporalKeys.add(TemporalKey.create(commitTimestamp, addedKey));
        }
        changedTemporalKeys.addAll(temporalKeyToNewValue.keySet());
        return changedTemporalKeys;
    }

    @Override
    public Collection<TemporalKey> datebackTransformValuesOfKeyspace(final String keyspace, final KeyspaceValueTransformation valueTransformation) {
        TemporalDataMatrix matrix = this.getMatrix(keyspace);
        if (matrix == null) {
            // the keyspace doesn't exist...
            return Collections.emptySet();
        }
        // TODO The code below is not very efficient if a lot of values change; the collection holding
        // the new data could grow to considerable sizes. However, it is the easiest way to implement this for now.
        Set<UnqualifiedTemporalEntry> newEntries = Sets.newHashSet();
        CloseableIterator<UnqualifiedTemporalEntry> iterator = matrix.allEntriesIterator(this.owningBranch.getBranchingTimestamp(), Long.MAX_VALUE);
        try {
            while (iterator.hasNext()) {
                UnqualifiedTemporalEntry entry = iterator.next();
                UnqualifiedTemporalKey temporalKey = entry.getKey();
                String key = temporalKey.getKey();
                long timestamp = temporalKey.getTimestamp();
                Object oldValue = this.deserialize(entry.getValue());
                if (oldValue == null) {
                    // do not alter deletion markers
                    continue;
                }
                Object newValue = valueTransformation.transformValue(key, timestamp, oldValue);
                if (newValue == null) {
                    throw new IllegalStateException("KeyspaceValueTransform unexpectedly returned NULL! It is not allowed to delete values!");
                }
                if (newValue == Dateback.UNCHANGED) {
                    // do not change this entry
                    continue;
                }
                UnqualifiedTemporalEntry newEntry = new UnqualifiedTemporalEntry(temporalKey, this.serialize(newValue));
                newEntries.add(newEntry);
            }
        } finally {
            iterator.close();
        }
        matrix.insertEntries(newEntries, true);
        return newEntries.stream().map(UnqualifiedTemporalEntry::getKey).map(k -> k.toTemporalKey(keyspace)).collect(Collectors.toSet());
    }

    @Override
    public void datebackUpdateCommitMetadata(final long commitTimestamp, final Object newMetadata) {
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        if (this.getCommitMetadataStore().hasCommitAt(commitTimestamp) == false) {
            throw new DatebackException("Cannot update commit metadata at timestamp " + commitTimestamp
                + ", because no commit has occurred there.");
        }
        this.getCommitMetadataStore().put(commitTimestamp, newMetadata);
    }

    @Override
    public void datebackCleanup(final String branch, long earliestTouchedTimestamp) {
        // in the basic implementation, we have nothing to do. Subclasses may override to perform backend-specific
        // cleanups for the given keys.
    }

    // =================================================================================================================
    // DUMP METHODS
    // =================================================================================================================

    @Override
    public CloseableIterator<ChronoDBEntry> allEntriesIterator(final long minTimestamp, final long maxTimestamp) {
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(minTimestamp <= maxTimestamp, "Precondition violation - argument 'minTimestamp' must be less than or equal to 'maxTimestamp'!");
        try (AutoLock lock = this.lockNonExclusive()) {
            return new AllEntriesIterator(minTimestamp, maxTimestamp);
        }
    }

    @Override
    public void insertEntries(final Set<ChronoDBEntry> entries, final boolean force) {
        try (AutoLock lock = this.lockBranchExclusive()) {
            // insertion of entries can (potentially) completely wreck the consistency of our cache.
            // in order to be safe, we clear it completely.
            this.getCache().clear();
            long now = 0L;
            try {
                now = this.getNow();
            } catch (Exception e) {
                // could not determine NOW timestamp -> we're loading a backup
            }
            long maxTimestamp = now;
            long branchingTimestamp = this.getOwningBranch().getBranchingTimestamp();
            SetMultimap<String, UnqualifiedTemporalEntry> keyspaceToEntries = HashMultimap.create();
            for (ChronoDBEntry entry : entries) {
                ChronoIdentifier chronoIdentifier = entry.getIdentifier();
                String keyspace = chronoIdentifier.getKeyspace();
                String key = chronoIdentifier.getKey();
                long timestamp = chronoIdentifier.getTimestamp();
                if (timestamp > System.currentTimeMillis()) {
                    throw new IllegalArgumentException(
                        "Cannot insert entries into database; at least one entry references a timestamp greater than the current system time!");
                }
                if (timestamp < branchingTimestamp) {
                    throw new IllegalArgumentException(
                        "Cannot insert entries into database; at least one entry references a timestamp smaller than the branching timestamp of this branch!");
                }
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
                matrix.insertEntries(entriesToInsert, force);
            }
            if (maxTimestamp > now) {
                this.setNow(maxTimestamp);
            }
        }
    }


    public void updateCreationTimestampForKeyspace(String keyspaceName, long creationTimestamp) {
        checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
        checkArgument(creationTimestamp >= 0, "Precondition violation - argument 'creationTimestamp' must not be negative!");
        TemporalDataMatrix matrix = this.getMatrix(keyspaceName);
        if (matrix != null) {
            matrix.ensureCreationTimestampIsGreaterThanOrEqualTo(creationTimestamp);
        }
    }

    // =================================================================================================================
    // DEBUG METHODS
    // =================================================================================================================

    @Override
    public void setDebugCallbackBeforePrimaryIndexUpdate(final Consumer<ChronoDBTransaction> action) {
        this.debugCallbackBeforePrimaryIndexUpdate = action;
    }

    @Override
    public void setDebugCallbackBeforeSecondaryIndexUpdate(final Consumer<ChronoDBTransaction> action) {
        this.debugCallbackBeforeSecondaryIndexUpdate = action;
    }

    @Override
    public void setDebugCallbackBeforeMetadataUpdate(final Consumer<ChronoDBTransaction> action) {
        this.debugCallbackBeforeMetadataUpdate = action;
    }

    @Override
    public void setDebugCallbackBeforeCacheUpdate(final Consumer<ChronoDBTransaction> action) {
        this.debugCallbackBeforeCacheUpdate = action;
    }

    @Override
    public void setDebugCallbackBeforeNowTimestampUpdate(final Consumer<ChronoDBTransaction> action) {
        this.debugCallbackBeforeNowTimestampUpdate = action;
    }

    @Override
    public void setDebugCallbackBeforeTransactionCommitted(final Consumer<ChronoDBTransaction> action) {
        this.debugCallbackBeforeTransactionCommitted = action;
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private ChronoDBCache getCache() {
        return this.owningDB.getCache();
    }

    @Override
    protected void verifyTransaction(final ChronoDBTransaction tx) {
        if (tx.getTimestamp() > this.getNow()) {
            throw new InvalidTransactionTimestampException(
                "Transaction timestamp (" + tx.getTimestamp() + ") must not be greater than timestamp of last commit (" + this.getNow() + ") on branch '" + this.getOwningBranch().getName() + "'!");
        }
        if (this.getOwningDB().getBranchManager().existsBranch(tx.getBranchName()) == false) {
            throw new InvalidTransactionBranchException(
                "The branch '" + tx.getBranchName() + "' does not exist at timestamp '" + tx.getTimestamp() + "'!");
        }
    }

    protected AtomicConflict scanForConflict(final ChronoDBTransaction tx, final long transactionCommitTimestamp,
                                             final String keyspace, final String key, final Object value) {
        Set<String> keyspaces = this.getKeyspaces(transactionCommitTimestamp);
        long now = this.getNow();
        if (tx.getTimestamp() == now) {
            // this transaction was started at the "now" timestamp. There has not been any commit
            // between starting this transaction and the current state. Therefore, there cannot
            // be any conflicts.
            return null;
        }
        if (keyspaces.contains(keyspace) == false) {
            // the keyspace is new, so blind overwrite can't happen
            return null;
        }
        // get the matrix representing the keyspace
        TemporalDataMatrix matrix = this.getMatrix(keyspace);
        if (matrix == null) {
            // the keyspace didn't even exist before, so no conflicts can happen
            return null;
        }
        // check when the last commit on that key has occurred
        long lastCommitTimestamp = -1;
        Object targetValue = null;
        String branch = tx.getBranchName();
        QualifiedKey qKey = QualifiedKey.create(keyspace, key);
        // first, try to find it in the cache
        CacheGetResult<Object> cacheGetResult = this.getCache().get(branch, now, qKey);
        if (cacheGetResult.isHit()) {
            // use the values from the cache
            lastCommitTimestamp = cacheGetResult.getValidFrom();
            targetValue = cacheGetResult.getValue();
        } else {
            // not in cache, load from store
            GetResult<?> getResult = this.performRangedGetInternal(branch, qKey, now, false);
            if (getResult.isHit()) {
                lastCommitTimestamp = getResult.getPeriod().getLowerBound();
                targetValue = getResult.getValue();
            }
        }
        // if the last commit timestamp was after our transaction, we have a potential conflict
        if (lastCommitTimestamp > tx.getTimestamp()) {
            ChronoIdentifier sourceKey = ChronoIdentifier.create(branch, transactionCommitTimestamp, qKey);
            ChronoIdentifier targetKey = ChronoIdentifier.create(branch, lastCommitTimestamp, qKey);
            return new AtomicConflictImpl(tx.getTimestamp(), sourceKey, value, targetKey, targetValue,
                this::findCommonAncestor);
        }
        // not conflicting
        return null;
    }

    /**
     * Returns the {@link TemporalDataMatrix} responsible for the given keyspace.
     *
     * @param keyspace The name of the keyspace to get the matrix for. Must not be <code>null</code>.
     * @return The temporal data matrix that stores the keyspace data, or <code>null</code> if there is no keyspace for
     * the given name.
     */
    public TemporalDataMatrix getMatrix(final String keyspace) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        return this.keyspaceToMatrix.get(keyspace);
    }

    /**
     * Returns the {@link TemporalDataMatrix} responsible for the given keyspace, creating it if it does not exist.
     *
     * @param keyspace  The name of the keyspace to get the matrix for. Must not be <code>null</code>.
     * @param timestamp In case of a "create", this timestamp specifies the creation timestamp of the matrix. Must not be
     *                  negative.
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

    protected void writeCommitThroughCache(final String branchName, final long timestamp,
                                           final Map<String, Map<String, Object>> keyspaceToKeyToValue) {
        // perform the write-through in our cache
        Map<QualifiedKey, Object> keyValues = Maps.newHashMap();
        boolean assumeImmutableValues = this.getOwningDB().getConfiguration().isAssumeCachedValuesAreImmutable();
        for (Entry<String, Map<String, Object>> outerEntry : keyspaceToKeyToValue.entrySet()) {
            String keyspace = outerEntry.getKey();
            Map<String, Object> keyToValue = outerEntry.getValue();
            for (Entry<String, Object> innerEntry : keyToValue.entrySet()) {
                String key = innerEntry.getKey();
                Object value = innerEntry.getValue();
                if (assumeImmutableValues == false) {
                    // values are not immutable, so we add a copy to the cache to prevent modification from outside
                    value = KryoManager.deepCopy(value);
                }
                QualifiedKey qKey = QualifiedKey.create(keyspace, key);
                keyValues.put(qKey, value);
            }
        }
        this.getCache().writeThrough(branchName, timestamp, keyValues);
    }

    @VisibleForTesting
    public void performRollbackToTimestamp(final long timestamp, final Set<String> modifiedKeyspaces,
                                           final boolean touchedIndex) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(modifiedKeyspaces, "Precondition violation - argument 'modifiedKeyspaces' must not be NULL!");
        for (String keyspace : modifiedKeyspaces) {
            TemporalDataMatrix matrix = this.getMatrix(keyspace);
            if (matrix != null) {
                matrix.rollback(timestamp);
            }
        }
        // roll back the commit metadata store
        this.getCommitMetadataStore().rollbackToTimestamp(timestamp);
        // roll back the cache
        this.getCache().rollbackToTimestamp(timestamp);
        // only rollback the index manager if we touched it during the commit
        if (touchedIndex) {
            this.getOwningDB().getIndexManager().rollback(this.getOwningBranch(), timestamp);
        }
        this.setNow(timestamp);
    }

    protected void assertThatTransactionMayPerformIncrementalCommit(final ChronoDBTransaction tx) {
        synchronized (this.incrementalCommitLock) {
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
        }
    }

    protected void assertThatTransactionMayPerformCommit(final ChronoDBTransaction tx) {
        synchronized (this.incrementalCommitLock) {
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
        }
    }

    protected boolean isFirstIncrementalCommit(final ChronoDBTransaction tx) {
        synchronized (this.incrementalCommitLock) {
            if (this.incrementalCommitTimestamp > 0) {
                // the incremental commit timestamp has already been decided -> this cannot be the first incremental
                // commit
                return false;
            } else {
                // the incremental commit timestamp has not yet been decided -> this is the first incremental commit
                return true;
            }
        }
    }

    protected void setUpIncrementalCommit(final ChronoDBTransaction tx) {
        synchronized (this.incrementalCommitLock) {
            this.incrementalCommitTimestamp = System.currentTimeMillis();
            // make sure we do not write to the same timestamp twice
            while (this.incrementalCommitTimestamp <= this.getNow()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                    // raise the interrupt flag again
                    Thread.currentThread().interrupt();
                }
                this.incrementalCommitTimestamp = System.currentTimeMillis();
            }
            this.incrementalCommitTransaction = tx;
        }
    }

    protected void terminateIncrementalCommitProcess() {
        synchronized (this.incrementalCommitLock) {
            this.incrementalCommitTimestamp = -1L;
            this.incrementalCommitTransaction = null;
        }
    }

    protected boolean isIncrementalCommitProcessOngoing() {
        synchronized (this.incrementalCommitLock) {
            if (this.incrementalCommitTimestamp >= 0) {
                return true;
            } else {
                return false;
            }
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

        MutableTransactionConfiguration txConfig = new DefaultTransactionConfiguration();
        txConfig.setBranch(this.owningBranch.getOrigin().getName());
        txConfig.setTimestamp(timestamp);
        // origin branch transactions are always deliberately in the past, therefore always readonly.
        txConfig.setReadOnly(true);
        // queries on the origin branch are always allowed during dateback
        txConfig.setAllowedDuringDateback(true);
        return this.owningDB.tx(txConfig);
    }

    protected Pair<ChronoIdentifier, Object> findCommonAncestor(final long transactionTimestamp,
                                                                final ChronoIdentifier source, final ChronoIdentifier target) {
        checkArgument(transactionTimestamp >= 0,
            "Precondition violation - argument 'transactionTimestamp' must not be negative!");
        checkNotNull(source, "Precondition violation - argument 'source' must not be NULL!");
        checkNotNull(target, "Precondition violation - argument 'target' must not be NULL!");
        checkArgument(source.toQualifiedKey().equals(target.toQualifiedKey()),
            "Precondition violation - arguments 'source' and 'target' do not specify the same qualified key, so there cannot be a common ancestor!");
        // perform a GET on the coordinates of the target, at the transaction timestamp of the inserting transaction
        GetResult<?> getResult = this.performRangedGetInternal(target.getBranchName(), target.toQualifiedKey(),
            transactionTimestamp, false);
        if (getResult.isHit() == false) {
            // no common ancestor
            return null;
        }
        long ancestorTimestamp = getResult.getPeriod().getLowerBound();
        // TODO: this is technically correct: if B branches away from A, then every entry in A is also
        // an entry in B. However, some client code might expect to see branch A here if the timestamp
        // is before the branching timestamp of B...
        String ancestorBranch = target.getBranchName();
        QualifiedKey ancestorQKey = getResult.getRequestedKey();
        ChronoIdentifier ancestorIdentifier = ChronoIdentifier.create(ancestorBranch,
            TemporalKey.create(ancestorTimestamp, ancestorQKey));
        Object ancestorValue = getResult.getValue();
        return Pair.of(ancestorIdentifier, ancestorValue);
    }


    @Override
    public BranchHeadStatistics calculateBranchHeadStatistics() {
        long now = this.getNow();
        long totalHead = 0;
        long totalHistory = 0;
        for (String keyspace : this.getKeyspaces(now)) {
            Set<String> keySet = this.performKeySet(this.getOwningBranch().getName(), now, keyspace);
            long headEntries = keySet.size();
            TemporalDataMatrix matrix = this.getMatrix(keyspace);
            long historyEntries;
            if (matrix == null) {
                historyEntries = 0;
            } else {
                historyEntries = matrix.size();
            }
            totalHead += headEntries;
            totalHistory += historyEntries;
        }
        return new BranchHeadStatisticsImpl(totalHead, totalHistory);
    }


    // =================================================================================================================
    // COMMIT HELPERS
    // =================================================================================================================

    private long waitForNextValidCommitTimestamp() {
        // make sure we do not write to the same timestamp twice
        return this.owningDB.getCommitTimestampProvider().getNextCommitTimestamp(this.getNow());
    }

    private ChangeSet analyzeChangeSet(
        final ChronoDBTransaction tx,
        final ChronoDBTransaction oldValueTx,
        final long time
    ) {
        ChangeSet changeSet = new ChangeSet();
        boolean duplicateVersionEliminationEnabled = tx.getConfiguration().getDuplicateVersionEliminationMode()
            .equals(DuplicateVersionEliminationMode.ON_COMMIT);
        ConflictResolutionStrategy conflictResolutionStrategy = tx.getConfiguration().getConflictResolutionStrategy();
        for (ChangeSetEntry entry : tx.getChangeSet()) {
            String keyspace = entry.getKeyspace();
            String key = entry.getKey();
            Object newValue = entry.getValue();
            Object oldValue = oldValueTx.get(keyspace, key);
            Set<PutOption> options = entry.getOptions();

            // conflict checking is not supported in incremental commit mode
            if (this.isIncrementalCommitProcessOngoing() == false) {
                if (duplicateVersionEliminationEnabled) {
                    if (Objects.equal(oldValue, newValue)) {
                        // the new value is identical to the old one -> ignore it
                        continue;
                    }
                }
                // check if conflicting with existing entry
                AtomicConflict conflict = this.scanForConflict(tx, time, keyspace, key, newValue);
                if (conflict != null) {
                    // resolve conflict
                    newValue = conflictResolutionStrategy.resolve(conflict);
                    // eliminate duplicates after resolving the conflict
                    if (Objects.equal(conflict.getTargetValue(), newValue)) {
                        // objects are identical after resolve, no need to commit the entry
                        continue;
                    }
                    // the "old value" is the previous value of our target now, because
                    // this is the timeline we merge into
                    oldValue = conflict.getTargetValue();
                }
            }
            if (entry.isRemove()) {
                changeSet.addEntry(keyspace, key, null);
            } else {
                changeSet.addEntry(keyspace, key, newValue);
            }

            ChronoIdentifier identifier = ChronoIdentifier.create(this.getOwningBranch(), time, keyspace, key);
            if (options.contains(PutOption.NO_INDEX) == false) {
                changeSet.addEntryToIndex(identifier, oldValue, newValue);
            }
        }
        return changeSet;
    }

    private void updatePrimaryIndex(String perfLogPrefix, final long time, final ChangeSet changeSet) {
        boolean performanceLoggingActive = this.owningDB.getConfiguration().isCommitPerformanceLoggingActive();
        SerializationManager serializer = this.getOwningDB().getSerializationManager();
        long beforeSerialization = System.currentTimeMillis();
        Iterable<Entry<String, Map<String, byte[]>>> serializedChangeSet = changeSet
            .getSerializedEntriesByKeyspace(serializer::serialize);
        if (performanceLoggingActive) {
            log.info(perfLogPrefix + " -> Serialize ChangeSet (" + changeSet.size() + "): " + (System.currentTimeMillis() - beforeSerialization) + "ms.");
        }
        for (Entry<String, Map<String, byte[]>> entry : serializedChangeSet) {
            String keyspace = entry.getKey();
            Map<String, byte[]> contents = entry.getValue();
            long beforeGetMatrix = System.currentTimeMillis();
            TemporalDataMatrix matrix = this.getOrCreateMatrix(keyspace, time);
            if (performanceLoggingActive) {
                log.info(perfLogPrefix + " -> Get Matrix for keyspace [" + keyspace + "]: " + (System.currentTimeMillis() - beforeGetMatrix) + "ms.");
            }
            long beforePut = System.currentTimeMillis();
            matrix.put(time, contents);
            if (performanceLoggingActive) {
                log.info(perfLogPrefix + " -> Put ChangeSet (" + changeSet.size() + ") into keyspace [" + keyspace + "]: " + (System.currentTimeMillis() - beforePut) + "ms.");
            }
        }
    }

    private boolean updateSecondaryIndices(final ChangeSet changeSet) {
        IndexManagerInternal indexManager = this.getOwningDB().getIndexManager();
        if (indexManager != null) {
            if (this.isIncrementalCommitProcessOngoing()) {
                // clear the query cache (if any). The reason for this is that during incremental updates,
                // we can get different results for the same query on the same timestamp. This is due to
                // changes in the same key at the timestamp of the incremental commit process.
                indexManager.clearQueryCache();
                // roll back the changed keys to the state before the incremental commit started
                Set<QualifiedKey> modifiedKeys = changeSet.getModifiedKeys();
                indexManager.rollback(this.getOwningBranch(), this.getNow(), modifiedKeys);
            }
            // re-index the modified keys
            indexManager.index(changeSet.getEntriesToIndex());
            return true;
        }
        // no secondary index manager present
        return false;
    }

    private void rollbackCurrentCommit(final ChangeSet changeSet, final boolean touchedIndex) {
        WriteAheadLogToken walToken = this.getWriteAheadLogTokenIfExists();
        Set<String> keyspaces = null;
        if (this.isIncrementalCommitProcessOngoing()) {
            // we are committing incrementally, no one knows which keyspaces were touched,
            // so we roll them all back just to be safe
            keyspaces = this.getAllKeyspaces();
        } else {
            // in a regular commit, only the keyspaces in our current transaction were touched
            keyspaces = changeSet.getModifiedKeyspaces();
        }
        this.performRollbackToTimestamp(walToken.getNowTimestampBeforeCommit(), keyspaces, touchedIndex);
        if (this.isIncrementalCommitProcessOngoing()) {
            // as a safety measure, we also have to clear the cache
            this.getCache().clear();
            // terminate the incremental commit process
            this.terminateIncrementalCommitProcess();
        }
        // after rolling back, we can clear the write ahead log
        this.clearWriteAheadLogToken();
    }

    // =================================================================================================================
    // DEBUG CALLBACKS
    // =================================================================================================================

    protected void debugCallbackBeforePrimaryIndexUpdate(final ChronoDBTransaction tx) {
        if (this.getOwningDB().getConfiguration().isDebugModeEnabled() == false) {
            return;
        }
        if (this.debugCallbackBeforePrimaryIndexUpdate != null) {
            this.debugCallbackBeforePrimaryIndexUpdate.accept(tx);
        }
    }

    protected void debugCallbackBeforeSecondaryIndexUpdate(final ChronoDBTransaction tx) {
        if (this.getOwningDB().getConfiguration().isDebugModeEnabled() == false) {
            return;
        }
        if (this.debugCallbackBeforeSecondaryIndexUpdate != null) {
            this.debugCallbackBeforeSecondaryIndexUpdate.accept(tx);
        }
    }

    protected void debugCallbackBeforeMetadataUpdate(final ChronoDBTransaction tx) {
        if (this.getOwningDB().getConfiguration().isDebugModeEnabled() == false) {
            return;
        }
        if (this.debugCallbackBeforeMetadataUpdate != null) {
            this.debugCallbackBeforeMetadataUpdate.accept(tx);
        }
    }

    protected void debugCallbackBeforeCacheUpdate(final ChronoDBTransaction tx) {
        if (this.getOwningDB().getConfiguration().isDebugModeEnabled() == false) {
            return;
        }
        if (this.debugCallbackBeforeCacheUpdate != null) {
            this.debugCallbackBeforeCacheUpdate.accept(tx);
        }
    }

    protected void debugCallbackBeforeNowTimestampUpdate(final ChronoDBTransaction tx) {
        if (this.getOwningDB().getConfiguration().isDebugModeEnabled() == false) {
            return;
        }
        if (this.debugCallbackBeforeNowTimestampUpdate != null) {
            this.debugCallbackBeforeNowTimestampUpdate.accept(tx);
        }
    }

    protected void debugCallbackBeforeTransactionCommitted(final ChronoDBTransaction tx) {
        if (this.getOwningDB().getConfiguration().isDebugModeEnabled() == false) {
            return;
        }
        if (this.debugCallbackBeforeTransactionCommitted != null) {
            this.debugCallbackBeforeTransactionCommitted.accept(tx);
        }
    }

    protected Object deserialize(final byte[] serialForm) {
        Object deserializedValue;
        if (serialForm == null || serialForm.length <= 0) {
            deserializedValue = null;
        } else {
            deserializedValue = this.getOwningDB().getSerializationManager().deserialize(serialForm);
        }
        return deserializedValue;
    }

    protected byte[] serialize(final Object object) {
        if (object == null) {
            return new byte[0];
        }
        return this.getOwningDB().getSerializationManager().serialize(object);
    }

    /**
     * Reads the entry at the given (exact) coordinates
     *
     * @param keyspace  The keyspace of the entry to retrieve. Must not be <code>null</code>.
     * @param key       The key of the entry to retrieve. Must not be <code>null</code>.
     * @param timestamp The exact timestamp of the entry to retrieve. Must not be <code>null</code>.
     * @return The {@link GetResult} of the operation, or <code>null</code> if the specified matrix cell is empty.
     */
    private GetResult<byte[]> readEntryAtCoordinates(final String keyspace, final String key, final long timestamp) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        TemporalDataMatrix matrix = this.getMatrix(keyspace);
        GetResult<byte[]> getResult = matrix.get(timestamp, key);
        if (getResult.isHit() == false || getResult.getPeriod().getLowerBound() != timestamp) {
            return null;
        }
        return getResult;
    }

    /**
     * Reads the entry at the given coordinates from the matrix and applies the given transformation.
     *
     * <p>
     * Important note: the entry will <b>not</b> be written back into the matrix after the transformation. However, the
     * caller is free to do so if desired.
     *
     * @param keyspace       The keyspace of the entry to retrieve and transform. Must not be <code>null</code>.
     * @param key            The key of the entry to retrieve and transform. Must not be <code>null</code>.
     * @param timestamp      The timestamp of the entry to retrieve and transform. Must match the entry <b>exactly</b>. Must not be
     *                       negative.
     * @param transformation The transformation to apply. Must not be <code>null</code>, must be a pure function. The function
     *                       receives the old value (which may be <code>null</code>, indicating that the entry is a deletion) and
     *                       returns the new value (again, <code>null</code> indicates a deletion). The function may also return
     *                       {@link Dateback#UNCHANGED} to indicate that the old value should not change.
     * @return The modified unqualified temporal entry, or <code>null</code> if the transformation function returned
     * {@link Dateback#UNCHANGED}.
     */
    private UnqualifiedTemporalEntry readAndTransformSingleEntryInMemory(final String keyspace, final String key,
                                                                         final long timestamp, final Function<Object, Object> transformation) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(transformation, "Precondition violation - argument 'transformation' must not be NULL!");
        GetResult<byte[]> getResult = this.readEntryAtCoordinates(keyspace, key, timestamp);
        if (getResult == null) {
            // the requested transformation cannot take place because there is no entry at the given coordinates
            throw new DatebackException("Cannot execute transformation: there is no entry at ["
                + this.getOwningBranch().getName() + "->" + keyspace + "->" + key + "@" + timestamp + "]!");
        }
        Object oldValue = this.deserialize(getResult.getValue());
        Object newValue = transformation.apply(oldValue);
        if (newValue == Dateback.UNCHANGED) {
            return null;
        }
        byte[] serializedNewValue = this.serialize(newValue);
        UnqualifiedTemporalEntry entry = new UnqualifiedTemporalEntry(UnqualifiedTemporalKey.create(key, timestamp),
            serializedNewValue);
        return entry;
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
     * @param timestamp The new timestamp to use as "now". Must not be negative.
     */
    protected abstract void setNow(long timestamp);

    /**
     * Creates a new {@link TemporalDataMatrix} for the given keyspace.
     *
     * @param keyspace  The name of the keyspace to create the matrix for. Must not be <code>null</code>.
     * @param timestamp The timestamp at which this keyspace was created. Must not be negative.
     * @return The newly created matrix instance for the given keyspace name. Never <code>null</code>.
     */
    protected abstract TemporalDataMatrix createMatrix(String keyspace, long timestamp);

    /**
     * Stores the given {@link WriteAheadLogToken} in the persistent store.
     *
     * <p>
     * Non-persistent stores can safely ignore this method.
     *
     * @param token The token to store. Must not be <code>null</code>.
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

        private final long minTimestamp;
        private final long maxTimestamp;
        private Iterator<String> keyspaceIterator;
        private String currentKeyspace;

        private CloseableIterator<UnqualifiedTemporalEntry> currentEntryIterator;

        public AllEntriesIterator(final long minTimestamp, final long maxTimestamp) {
            AbstractTemporalKeyValueStore self = AbstractTemporalKeyValueStore.this;
            Set<String> keyspaces = Sets.newHashSet(self.getKeyspaces(maxTimestamp));
            this.keyspaceIterator = keyspaces.iterator();
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
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
                this.currentEntryIterator = matrix.allEntriesIterator(this.minTimestamp, this.maxTimestamp);
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
            return ChronoDBEntry.create(chronoIdentifier, actualValue);
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
