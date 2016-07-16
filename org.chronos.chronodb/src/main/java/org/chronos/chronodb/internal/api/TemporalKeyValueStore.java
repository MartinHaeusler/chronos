package org.chronos.chronodb.internal.api;

import java.util.Iterator;
import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.TransactionSource;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.ChronoDBInternal.ChronoNonReturningJob;
import org.chronos.chronodb.internal.api.ChronoDBInternal.ChronoReturningJob;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;

/**
 * A {@link TemporalKeyValueStore} (or <i>TKVS</i>) is a collection of named keyspaces, which in turn contain temporal
 * key-value pairs.
 *
 * <p>
 * By default, each non-empty keyspace is associated with a {@link TemporalDataMatrix} which performs the actual
 * operations. This class serves as a manager object that forwards the incoming requests to the correct matrix, based
 * upon the keyspace name.
 *
 * <p>
 * This class also manages the metadata of a TKVS, in particular the name of the branch to which this TKVS corresponds,
 * the <i>now</i> timestamp, and the index manager used by this TKVS.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface TemporalKeyValueStore extends TransactionSource {

	// =================================================================================================================
	// RECOVERY
	// =================================================================================================================

	/**
	 * This method performs recovery operations on this store in case that it was not properly shut down last time.
	 *
	 * <p>
	 * If the last shutdown was a regular shutdown, this method will only perform some quick checks and then return. If
	 * the last shutdown occurred unexpectedly (e.g. during a commit), a recovery operation will be executed.
	 */
	public void performStartupRecoveryIfRequired();

	// =================================================================================================================
	// METADATA
	// =================================================================================================================

	/**
	 * Returns the {@link ChronoDB} instance which owns this store.
	 *
	 * @return The owning database. Never <code>null</code>.
	 */
	public ChronoDBInternal getOwningDB();

	/**
	 * Returns the cache which is currently used by this TKVS instance.
	 *
	 * @return The currently used cache. May be <code>null</code> if no cache is in use.
	 */
	public ChronoDBCache getCache();

	/**
	 * Sets the cache to use in this instance.
	 *
	 * <p>
	 * This method will call {@link ChronoDBCache#clear()} on the given cache before using it. The given cache instance
	 * must not be used by any other TKVS.
	 *
	 * @param cache
	 *            The cache to use. May be <code>null</code> to turn off caching for this instance. Caches must not be
	 *            shared among multiple TKVS instances!
	 */
	public void setCache(ChronoDBCache cache);

	/**
	 * Returns the <i>now</i> timestamp.
	 *
	 * <p>
	 * The point in time we refer to as <i>now</i> is defined as the <i>last</i> timestamp where a successful commit on
	 * this store (in any keyspace) has taken place. Therefore, <i>now</i> is always less than (or equal to)
	 * {@link System#currentTimeMillis()}. It is a logical point in time which does not necessarily correspond to the
	 * current wall clock time.
	 *
	 * <p>
	 * In general, transactions are <b>not allowed to be opened after the <i>now</i> timestamp</b> in order to prevent
	 * temporal anomalies.
	 *
	 * @return The <i>now</i> timestamp, i.e. the timestamp of the last successful commit on this store. Never negative.
	 */
	public long getNow();

	/**
	 * Returns the set of known keyspace names which are contained in this store.
	 *
	 * <p>
	 * The returned set represents the known keyspaces at the given point in time. The set itself is immutable. Calling
	 * this operation on different timestamps will produce different sets.
	 *
	 * @param tx
	 *            The transaction to operate on. Must not be <code>null</code>.
	 *
	 * @return An immutable set, containing the names of all keyspaces contained in this store.
	 */
	public Set<String> getKeyspaces(ChronoDBTransaction tx);

	/**
	 * Returns the set of known keyspace names which are contained in this store at the given point in time.
	 *
	 * <p>
	 * The returned set represents the known keyspaces at the given point in time. The set itself is immutable. Calling
	 * this operation on different timestamps will produce different sets.
	 *
	 * @param timestamp
	 *            The timestamp to operate on. Must not be negative.
	 *
	 * @return An immutable set, containing the names of all keyspaces contained in this store.
	 */
	public Set<String> getKeyspaces(long timestamp);

	/**
	 * Returns the set of all keys that ever existed in the given keyspace, regardless of their timestamp.
	 *
	 * <p>
	 * For non-existent keyspaces, this method returns the empty set.
	 *
	 * @param keyspace
	 *            The keyspace to get the keys for. Must not be <code>null</code>.
	 * @return The set of keys that ever existed in the given keyspace. May be empty, but never <code>null</code>.
	 */
	public Set<String> getAllKeys(String keyspace);

	/**
	 * Returns the {@link Branch} to which this key-value store belongs.
	 *
	 * @return The owning branch. Never <code>null</code>.
	 */
	public Branch getOwningBranch();

	// =====================================================================================================================
	// TRANSACTION BUILDING
	// =====================================================================================================================

	public ChronoDBTransaction tx(TransactionConfigurationInternal configuration);

	/**
	 * Opens a transaction for internal use.
	 *
	 * <p>
	 * This transaction has significantly fewer preconditions and safety checks for the sake of greater flexibility.
	 *
	 * @param branchName
	 *            The branch to operate on. Must not be <code>null</code>, must refer to an existing branch.
	 * @param timestamp
	 *            The timestamp to operate on. Must not be negative.
	 *
	 * @return The new internal transaction. Never <code>null</code>.
	 */
	public ChronoDBTransaction txInternal(String branchName, long timestamp);

	// =================================================================================================================
	// TRANSACTION OPERATION EXECUTION
	// =================================================================================================================

	/**
	 * Performs the actual commit operation for the given transaction on this store.
	 *
	 * <p>
	 * During this process, the change set (see: {@link ChronoDBTransaction#getChangeSet()}) will be written to the
	 * actual backing data store, abiding the ACID rules.
	 *
	 * @param tx
	 *            The transaction to commit. Must not be <code>null</code>.
	 * @param commitMetadata
	 *            The metadata object to store alongside the commit. May be <code>null</code>. Will be serialized using
	 *            the {@link SerializationManager} associated with this {@link ChronoDB} instance.
	 */
	public void performCommit(ChronoDBTransaction tx, Object commitMetadata);

	/**
	 * Performs an incremental commit, using the given transaction.
	 *
	 * <p>
	 * For more details on the incremental commit process, please see {@link ChronoDBTransaction#commitIncremental()}.
	 *
	 * @param tx
	 *            The transaction to perform the incremental commit for. Must not be <code>null</code>.
	 *
	 * @return The new timestamp for the given transaction.
	 *
	 * @throws ChronoDBCommitException
	 *             Thrown if the incremental commit fails. If this exception is thrown, any changes made by the
	 *             incremental commit process will have been rolled back.
	 */
	public long performCommitIncremental(ChronoDBTransaction tx) throws ChronoDBCommitException;

	/**
	 * Performs a rollback on an inremental commit process started by the given transaction.
	 *
	 * <p>
	 * This operation will cancel the incremental commit process and roll back the data store to the state it has been
	 * in before the incremental commit started.
	 *
	 * @param tx
	 *            The transaction that started the incremental commit to roll back. Must not be <code>null</code>.
	 */
	public void performIncrementalRollback(ChronoDBTransaction tx);

	/**
	 * Performs an actual <code>get</code> operation on this key-value store, in the given transaction.
	 *
	 * <p>
	 * A <code>get</code> operation finds the key-value entry <code>E</code> for a given {@link QualifiedKey}
	 * <code>K</code> where:
	 * <ul>
	 * <li>The keyspace and key in <code>K</code> are equal to the ones in <code>E</code>
	 * <li>The timestamp of <code>E</code> is the largest timestamps among all entries that match the other condition,
	 * and is less than or equal to the timestamp of the transaction
	 * </ul>
	 *
	 * <p>
	 * ... in other words, this method finds the result of the <i>latest</i> commit operation on the given key, up to
	 * (and including) the timestamp which is specified by {@link ChronoDBTransaction#getTimestamp()}.
	 *
	 *
	 * @param tx
	 *            The transaction in which this operation takes place. Must not be <code>null</code>.
	 * @param key
	 *            The qualified key to look up in the store. Must not be <code>null</code>.
	 * @return The value for the key in this store at the timestamp specified by the transaction. May be
	 *         <code>null</code> to indicate that either this key was never written, was not yet written at the given
	 *         time, or has been removed (and not been re-added) before the given time.
	 */
	public Object performGet(ChronoDBTransaction tx, QualifiedKey key);

	/**
	 * This operation is equivalent to {@link #performGet(ChronoDBTransaction, QualifiedKey)}, but produces additional
	 * result data.
	 *
	 * <p>
	 * A regular <code>get</code> operation only retrieves the value which was valid in the store at the given point in
	 * time. This method will also retrieve a {@link Period} indicating from which timestamp to which other timestamp
	 * the returned value is valid.
	 *
	 * <p>
	 * The result object of this method is essentially just a wrapper for the actual <code>get</code> result, and a
	 * validity period.
	 *
	 * <p>
	 * <b>Important note:</b> The upper bound of the validity period may <b>exceed</b> the transaction timestamp! It
	 * reflects the actual temporal validity range of the key-value pair. This implies a number of special cases:
	 * <ul>
	 * <li>If a key-value pair is the latest entry for a given key (i.e. it has not yet been overridden by any other
	 * commit), the value of the period's upper bound will be {@link Long#MAX_VALUE} to represent infinity.
	 * <li>If there is no entry for a given key, the returned period will be the "eternal" period (i.e. ranging from
	 * zero to {@link Long#MAX_VALUE}), and the result value will be <code>null</code>.
	 * <li>If all entries for a given key are inserted after the requested timestamp, the returned period will start at
	 * zero and end at the timestamp where the first key-value pair for the requested key was inserted. The result value
	 * will be <code>null</code> in this case.
	 * <li>If the last operation that occurred on the key before the requested timestamp was a remove operation, the
	 * result value will be <code>null</code>. The period will start at the timestamp where the remove operation
	 * occurred. If the key was written again at a later point in time, the period will be terminated at the timestamp
	 * of that commit, otherwise the upper bound of the period will be {@link Long#MAX_VALUE}.
	 * </ul>
	 *
	 *
	 *
	 * @param tx
	 *            The transaction on which this operation occurs. Must not be <code>null</code>.
	 * @param key
	 *            The qualified key to search for. Must not be <code>null</code>.
	 *
	 * @return A {@link RangedGetResult} object, containing the actual result value and a temporal validity range as
	 *         described above. Never <code>null</code>.
	 */
	public RangedGetResult<Object> performRangedGet(ChronoDBTransaction tx, QualifiedKey key);

	/**
	 * Retrieves the set of keys contained in this store in the given keyspace at the given point in time.
	 *
	 * <p>
	 * Please keep in mind that the key set can not only grow (as new keys are being added), but can also shrink when
	 * keys are removed. The key set only reflects the keys which have valid (non-<code>null</code>) values at the given
	 * point in time.
	 *
	 * @param tx
	 *            The transaction on which this operation occurs. Must not be <code>null</code>.
	 * @param keyspaceName
	 *            The name of the keyspace to retrieve the key set for. Must not be <code>null</code>.
	 *
	 * @return The key set of the given keyspace, at the point in time determined by the given transaction. May be
	 *         empty, but never <code>null</code>. If the given keyspace is unknown (i.e. does not exist yet), the empty
	 *         set will be returned.
	 */
	public Set<String> performKeySet(ChronoDBTransaction tx, String keyspaceName);

	/**
	 * Returns the history of the given key in this store, up to and including the timestamp of the given transaction.
	 *
	 * <p>
	 * The history is represented by a number of timestamps. At each of these timestamps, a successful commit operation
	 * has modified the value for the given key (and possibly also other key-value pairs).
	 *
	 * <p>
	 * The result of this operation is an iterator which iterates over the change timestamps in descending order (i.e.
	 * latest changes first). Only commits which have happened before the given transaction (i.e. have timestamps less
	 * than or equal to the given transaction timestamp) will be considered as candidates. The iterator may be empty if
	 * there are no entries in the history of the given key up to the timestamp of the given transaction (i.e. the key
	 * is unknown up to and including the timestamp of the given transaction).
	 *
	 * @param tx
	 *            The transaction on which this operation occurs. Must not be <code>null</code>.
	 * @param key
	 *            The key to retrieve the history (change timestamps) for. Must not be <code>null</code>.
	 * @return An iterator over all change timestamps (descending order; latest changes first) up to (and including) the
	 *         timestamp of the given transaction, as specified above. May be empty, but never <code>null</code>.
	 */
	public Iterator<Long> performHistory(ChronoDBTransaction tx, QualifiedKey key);

	/**
	 * Returns an iterator over the modified keys in the given timestamp range.
	 *
	 * <p>
	 * Please note that <code>timestampLowerBound</code> and <code>timestampUpperBound</code> must not be larger than
	 * the timestamp of the transaction, i.e. you can only look for timestamp ranges in the past with this method.
	 *
	 * @param tx
	 *            The transaction to work with. Must not be <code>null</code>.
	 * @param keyspace
	 *            The keyspace to look for changes in. Must not be <code>null</code>. For non-existing keyspaces, the
	 *            resulting iterator will be empty.
	 * @param timestampLowerBound
	 *            The lower bound on the time range to look for. Must not be negative. Must be less than or equal to
	 *            <code>timestampUpperBound</code>. Must be less than or equal to the timestamp of this transaction.
	 * @param timestampUpperBound
	 *            The upper bound on the time range to look for. Must not be negative. Must be greater than or equal to
	 *            <code>timestampLowerBound</code>. Must be less than or equal to the timestamp of this transaction.
	 * @return An iterator containing the {@link TemporalKey}s that reflect the modifications. May be empty, but never
	 *         <code>null</code>.
	 */
	public Iterator<TemporalKey> performGetModificationsInKeyspaceBetween(ChronoDBTransaction tx, String keyspace,
			long timestampLowerBound, long timestampUpperBound);

	/**
	 * Returns the metadata object stored alongside the commit at the given timestamp.
	 *
	 * <p>
	 * It is important that the given timestamp matches the commit timestamp <b>exactly</b>.
	 *
	 * @param tx
	 *            The transaction to work with. Must not be <code>null</code>.
	 * @param commitTimestamp
	 *            The commit timestamp to get the metadata object for. Must match the commit timestamp exactly. Must not
	 *            be negative. Must be less than or equal to the timestamp of this transaction (i.e. must be in the
	 *            past).
	 *
	 * @return The object stored alongside that commit. Will be <code>null</code> for all timestamps that are not
	 *         associated with a commit. May also be <code>null</code> in cases where no metadata object was given for
	 *         the commit by the user.
	 */
	public Object performGetCommitMetadata(ChronoDBTransaction tx, long commitTimestamp);

	// =================================================================================================================
	// BRANCH LOCKING
	// =================================================================================================================

	/**
	 * Performs a non-exclusive operation on the branch which can run in parallel with other non-exclusive jobs.
	 *
	 * <p>
	 * This method ensures that non-exclusive operations are properly blocked when an exclusive operation is taking
	 * place.
	 *
	 * <p>
	 * This method <b>also acquires the non-exclusive lock</b> on the whole database!
	 *
	 * <p>
	 * This variant of this method returns a value; if you don't need a return value, please see
	 * {@link #performNonExclusive(ChronoNonReturningJob)}.
	 *
	 * <p>
	 * It is strongly encouraged to use Java Lambda Expressions as arguments for this method.
	 *
	 * @param <T>
	 *            The type of object the job returns.
	 * @param job
	 *            The job to execute. Must not be <code>null</code>.
	 *
	 * @return The result of the job. May be <code>null</code>.
	 */
	public <T> T performNonExclusive(final ChronoReturningJob<T> job);

	/**
	 * Performs a non-exclusive operation on the branch which can run in parallel with other non-exclusive jobs.
	 *
	 * <p>
	 * This method ensures that non-exclusive operations are properly blocked when an exclusive operation is taking
	 * place.
	 *
	 * <p>
	 * This method <b>also acquires the non-exclusive lock</b> on the whole database!
	 *
	 * <p>
	 * This variant of this method does not return a value; if you need a return value, please see
	 * {@link #performNonExclusive(ChronoReturningJob)}.
	 *
	 * <p>
	 * It is strongly encouraged to use Java Lambda Expressions as arguments for this method.
	 *
	 * @param job
	 *            The job to execute. Must not be <code>null</code>.
	 */
	public void performNonExclusive(final ChronoNonReturningJob job);

	/**
	 * Performs an exclusive operation on the branch which prevents all other jobs on the branch from executing while it
	 * is active.
	 *
	 * <p>
	 * While this method is being executed, no other jobs (exlusive or non-exclusive) are started.
	 *
	 * <p>
	 * This method <b>also acquires the non-exclusive lock</b> on the whole database!
	 *
	 * <p>
	 * This variant of this method does not return a value; if you need a return value, please see
	 * {@link #performBranchExclusive(ChronoNonReturningJob)}.
	 *
	 * <p>
	 * It is strongly encouraged to use Java Lambda Expressions as arguments for this method.
	 *
	 * @param job
	 *            The job to execute. Must not be <code>null</code>.
	 */
	public void performBranchExclusive(final ChronoNonReturningJob job);

	/**
	 * Performs an exclusive operation on the branch which prevents all other jobs on the branch from executing while it
	 * is active.
	 *
	 * <p>
	 * While this method is being executed, no other jobs (exlusive or non-exclusive) are started.
	 *
	 * <p>
	 * This method <b>also acquires the non-exclusive lock</b> on the whole database!
	 *
	 * <p>
	 * This variant of this method returns a value; if you don't need a return value, please see
	 * {@link #performBranchExclusive(ChronoNonReturningJob)}.
	 *
	 * <p>
	 * It is strongly encouraged to use Java Lambda Expressions as arguments for this method.
	 *
	 * @param <T>
	 *            The type of object the job returns.
	 * @param job
	 *            The job to execute. Must not be <code>null</code>.
	 *
	 * @return The result of the job. May be <code>null</code>.
	 */
	public <T> T performBranchExclusive(final ChronoReturningJob<T> job);

	// =================================================================================================================
	// DUMP UTILITY
	// =================================================================================================================

	/**
	 * Returns an iterator over all entries in this data store which have timestamps less than or equal to the given
	 * timestamp.
	 *
	 * <p>
	 * This method is intended primarily for operations that perform backup work on the database. It is <b>strongly
	 * discouraged</b> to perform any kind of filtering or analysis on the returned iterator (for performance reasons);
	 * use one of the other retrieval methods provided by this class instead.
	 *
	 * <p>
	 * <b>/!\ WARNING /!\</b><br>
	 * This method will return only entries from <b>this</b> branch! Entries from the origin branch (if any) will
	 * <b>not</b> be included in the returned iterator!
	 *
	 * <p>
	 * <b>/!\ WARNING /!\</b><br>
	 * The resulting iterator <b>must</b> be {@linkplain CloseableIterator#close() closed} by the caller!
	 *
	 * @param maxTimestamp
	 *            The maximum timestamp to consider. Only entries with timestamps less than or equal to this value will
	 *            be returned. Must not be negative.
	 *
	 * @return An iterator over all entries with timestamps less than or equal to the given timestamp. May be empty, but
	 *         never <code>null</code>. Must be closed by the caller.
	 */
	public CloseableIterator<ChronoDBEntry> allEntriesIterator(long maxTimestamp);

	/**
	 * Directly inserts the given entries into this store, without performing any temporal consistency checks.
	 *
	 * <p>
	 * This method is intended to be used only for loading a previously stored backup, and is <b>strongly
	 * discouraged</b> for regular use. Please use transactions and commits instead to ensure temporal consistency.
	 *
	 * @param entries
	 *            The set of entries to insert. Must not be <code>null</code>. If the set is empty, this method is
	 *            effectively a no-op.
	 */
	public void insertEntries(Set<ChronoDBEntry> entries);

}
