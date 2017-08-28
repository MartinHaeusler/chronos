package org.chronos.chronodb.internal.api;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.TransactionSource;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;

/**
 * A {@link TemporalKeyValueStore} (or <i>TKVS</i>) is a collection of named keyspaces, which in turn contain temporal key-value pairs.
 *
 * <p>
 * By default, each non-empty keyspace is associated with a {@link TemporalDataMatrix} which performs the actual operations. This class serves as a manager object that forwards the incoming requests to the correct matrix, based upon the keyspace name.
 *
 * <p>
 * This class also manages the metadata of a TKVS, in particular the name of the branch to which this TKVS corresponds, the <i>now</i> timestamp, and the index manager used by this TKVS.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface TemporalKeyValueStore extends TransactionSource, Lockable {

	// =================================================================================================================
	// RECOVERY
	// =================================================================================================================

	/**
	 * This method performs recovery operations on this store in case that it was not properly shut down last time.
	 *
	 * <p>
	 * If the last shutdown was a regular shutdown, this method will only perform some quick checks and then return. If the last shutdown occurred unexpectedly (e.g. during a commit), a recovery operation will be executed.
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
	 * Returns the <i>now</i> timestamp.
	 *
	 * <p>
	 * The point in time we refer to as <i>now</i> is defined as the <i>last</i> timestamp where a successful commit on this store (in any keyspace) has taken place. Therefore, <i>now</i> is always less than (or equal to) {@link System#currentTimeMillis()}. It is a logical point in time which does not necessarily correspond to the current wall clock time.
	 *
	 * <p>
	 * In general, transactions are <b>not allowed to be opened after the <i>now</i> timestamp</b> in order to prevent temporal anomalies.
	 *
	 * @return The <i>now</i> timestamp, i.e. the timestamp of the last successful commit on this store. Never negative.
	 */
	public long getNow();

	/**
	 * Returns the set of known keyspace names which are contained in this store.
	 *
	 * <p>
	 * The returned set represents the known keyspaces at the given point in time. The set itself is immutable. Calling this operation on different timestamps will produce different sets.
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
	 * The returned set represents the known keyspaces at the given point in time. The set itself is immutable. Calling this operation on different timestamps will produce different sets.
	 *
	 * @param timestamp
	 *            The timestamp to operate on. Must not be negative.
	 *
	 * @return An immutable set, containing the names of all keyspaces contained in this store.
	 */
	public Set<String> getKeyspaces(long timestamp);

	/**
	 * Returns the {@link Branch} to which this key-value store belongs.
	 *
	 * @return The owning branch. Never <code>null</code>.
	 */
	public Branch getOwningBranch();

	/**
	 * Returns the {@link CommitMetadataStore} to work with in this store.
	 *
	 * @return The commit metadata store associated with this store. Never <code>null</code>.
	 */
	public CommitMetadataStore getCommitMetadataStore();

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
	 * During this process, the change set (see: {@link ChronoDBTransaction#getChangeSet()}) will be written to the actual backing data store, abiding the ACID rules.
	 *
	 * @param tx
	 *            The transaction to commit. Must not be <code>null</code>.
	 * @param commitMetadata
	 *            The metadata object to store alongside the commit. May be <code>null</code>. Will be serialized using the {@link SerializationManager} associated with this {@link ChronoDB} instance.
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
	 *             Thrown if the incremental commit fails. If this exception is thrown, any changes made by the incremental commit process will have been rolled back.
	 */
	public long performCommitIncremental(ChronoDBTransaction tx) throws ChronoDBCommitException;

	/**
	 * Performs a rollback on an inremental commit process started by the given transaction.
	 *
	 * <p>
	 * This operation will cancel the incremental commit process and roll back the data store to the state it has been in before the incremental commit started.
	 *
	 * @param tx
	 *            The transaction that started the incremental commit to roll back. Must not be <code>null</code>.
	 */
	public void performIncrementalRollback(ChronoDBTransaction tx);

	/**
	 * Performs an actual <code>get</code> operation on this key-value store, in the given transaction.
	 *
	 * <p>
	 * A <code>get</code> operation finds the key-value entry <code>E</code> for a given {@link QualifiedKey} <code>K</code> where:
	 * <ul>
	 * <li>The keyspace and key in <code>K</code> are equal to the ones in <code>E</code>
	 * <li>The timestamp of <code>E</code> is the largest timestamps among all entries that match the other condition, and is less than or equal to the timestamp of the transaction
	 * </ul>
	 *
	 * <p>
	 * ... in other words, this method finds the result of the <i>latest</i> commit operation on the given key, up to (and including) the timestamp which is specified by {@link ChronoDBTransaction#getTimestamp()}.
	 *
	 *
	 * @param tx
	 *            The transaction in which this operation takes place. Must not be <code>null</code>.
	 * @param key
	 *            The qualified key to look up in the store. Must not be <code>null</code>.
	 * @return The value for the key in this store at the timestamp specified by the transaction. May be <code>null</code> to indicate that either this key was never written, was not yet written at the given time, or has been removed (and not been re-added) before the given time.
	 */
	public Object performGet(ChronoDBTransaction tx, QualifiedKey key);

	/**
	 * This operation is equivalent to {@link #performGet(ChronoDBTransaction, QualifiedKey)}, but produces additional result data.
	 *
	 * <p>
	 * A regular <code>get</code> operation only retrieves the value which was valid in the store at the given point in time. This method will also retrieve a {@link Period} indicating from which timestamp to which other timestamp the returned value is valid.
	 *
	 * <p>
	 * The result object of this method is essentially just a wrapper for the actual <code>get</code> result, and a validity period.
	 *
	 * <p>
	 * <b>Important note:</b> The upper bound of the validity period may <b>exceed</b> the transaction timestamp! It reflects the actual temporal validity range of the key-value pair. This implies a number of special cases:
	 * <ul>
	 * <li>If a key-value pair is the latest entry for a given key (i.e. it has not yet been overridden by any other commit), the value of the period's upper bound will be {@link Long#MAX_VALUE} to represent infinity.
	 * <li>If there is no entry for a given key, the returned period will be the "eternal" period (i.e. ranging from zero to {@link Long#MAX_VALUE}), and the result value will be <code>null</code>.
	 * <li>If all entries for a given key are inserted after the requested timestamp, the returned period will start at zero and end at the timestamp where the first key-value pair for the requested key was inserted. The result value will be <code>null</code> in this case.
	 * <li>If the last operation that occurred on the key before the requested timestamp was a remove operation, the result value will be <code>null</code>. The period will start at the timestamp where the remove operation occurred. If the key was written again at a later point in time, the period will be terminated at the timestamp of that commit, otherwise the upper bound of the period will be {@link Long#MAX_VALUE}.
	 * </ul>
	 *
	 *
	 *
	 * @param tx
	 *            The transaction on which this operation occurs. Must not be <code>null</code>.
	 * @param key
	 *            The qualified key to search for. Must not be <code>null</code>.
	 *
	 * @return A {@link GetResult} object, containing the actual result value and a temporal validity range as described above. Never <code>null</code>.
	 */
	public GetResult<Object> performRangedGet(ChronoDBTransaction tx, QualifiedKey key);

	/**
	 * Retrieves the set of keys contained in this store in the given keyspace at the given point in time.
	 *
	 * <p>
	 * Please keep in mind that the key set can not only grow (as new keys are being added), but can also shrink when keys are removed. The key set only reflects the keys which have valid (non-<code>null</code>) values at the given point in time.
	 *
	 * @param tx
	 *            The transaction on which this operation occurs. Must not be <code>null</code>.
	 * @param keyspaceName
	 *            The name of the keyspace to retrieve the key set for. Must not be <code>null</code>.
	 *
	 * @return The key set of the given keyspace, at the point in time determined by the given transaction. May be empty, but never <code>null</code>. If the given keyspace is unknown (i.e. does not exist yet), the empty set will be returned.
	 */
	public Set<String> performKeySet(ChronoDBTransaction tx, String keyspaceName);

	/**
	 * Returns the history of the given key in this store, up to and including the timestamp of the given transaction.
	 *
	 * <p>
	 * The history is represented by a number of timestamps. At each of these timestamps, a successful commit operation has modified the value for the given key (and possibly also other key-value pairs).
	 *
	 * <p>
	 * The result of this operation is an iterator which iterates over the change timestamps in descending order (i.e. latest changes first). Only commits which have happened before the given transaction (i.e. have timestamps less than or equal to the given transaction timestamp) will be considered as candidates. The iterator may be empty if there are no entries in the history of the given key up to the timestamp of the given transaction (i.e. the key is unknown up to and including the timestamp of the given transaction).
	 *
	 * @param tx
	 *            The transaction on which this operation occurs. Must not be <code>null</code>.
	 * @param key
	 *            The key to retrieve the history (change timestamps) for. Must not be <code>null</code>.
	 * @return An iterator over all change timestamps (descending order; latest changes first) up to (and including) the timestamp of the given transaction, as specified above. May be empty, but never <code>null</code>.
	 */
	public Iterator<Long> performHistory(ChronoDBTransaction tx, QualifiedKey key);

	/**
	 * Returns an iterator over the modified keys in the given timestamp range.
	 *
	 * <p>
	 * Please note that <code>timestampLowerBound</code> and <code>timestampUpperBound</code> must not be larger than the timestamp of the transaction, i.e. you can only look for timestamp ranges in the past with this method.
	 *
	 * @param tx
	 *            The transaction to work with. Must not be <code>null</code>.
	 * @param keyspace
	 *            The keyspace to look for changes in. Must not be <code>null</code>. For non-existing keyspaces, the resulting iterator will be empty.
	 * @param timestampLowerBound
	 *            The lower bound on the time range to look for. Must not be negative. Must be less than or equal to <code>timestampUpperBound</code>. Must be less than or equal to the timestamp of this transaction.
	 * @param timestampUpperBound
	 *            The upper bound on the time range to look for. Must not be negative. Must be greater than or equal to <code>timestampLowerBound</code>. Must be less than or equal to the timestamp of this transaction.
	 * @return An iterator containing the {@link TemporalKey}s that reflect the modifications. May be empty, but never <code>null</code>.
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
	 *            The commit timestamp to get the metadata object for. Must match the commit timestamp exactly. Must not be negative. Must be less than or equal to the timestamp of this transaction (i.e. must be in the past).
	 *
	 * @return The object stored alongside that commit. Will be <code>null</code> for all timestamps that are not associated with a commit. May also be <code>null</code> in cases where no metadata object was given for the commit by the user.
	 */
	public Object performGetCommitMetadata(ChronoDBTransaction tx, long commitTimestamp);

	/**
	 * Returns an iterator over all timestamps where commits have occurred, bounded between <code>from</code> and <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty iterator.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>.
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned timestamps. Must not be <code>null</code>.
	 * @return The iterator over the commit timestamps in the given time range. May be empty, but never <code>null</code>.
	 */
	public Iterator<Long> performGetCommitTimestampsBetween(ChronoDBTransaction tx, long from, long to, Order order);

	/**
	 * Returns an iterator over the entries of commit timestamp and associated metadata, bounded between <code>from</code> and <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty iterator.
	 *
	 * <p>
	 * Please keep in mind that some commits may not have any metadata attached. In this case, the {@linkplain Entry#getValue() value} component of the {@link Entry} will be set to <code>null</code>.
	 *
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>.
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned commits. Must not be <code>null</code>.
	 *
	 * @return An iterator over the commits in the given time range. The contained entries have the timestamp as the {@linkplain Entry#getKey() key} component and the associated metadata as their {@linkplain Entry#getValue() value} component (which may be <code>null</code>). May be empty, but never <code>null</code>.
	 */
	public Iterator<Entry<Long, Object>> performGetCommitMetadataBetween(ChronoDBTransaction tx, long from, long to,
			Order order);

	/**
	 * Returns an iterator over commit timestamps in a paged fashion.
	 *
	 * <p>
	 * For example, calling {@code getCommitTimestampsPaged(10000, 100, 0, Order.DESCENDING)} will give the latest 100 commit timestamps that have occurred before timestamp 10000. Calling {@code getCommitTimestampsPaged(123456, 200, 2, Order.DESCENDING} will return 200 commit timestamps, skipping the 400 latest commit timestamps, which are smaller than 123456.
	 *
	 * @param tx
	 *            The transaction to use. Must not be <code>null</code>.
	 * @param minTimestamp
	 *            The minimum timestamp to consider (inclusive). All lower timestamps will be excluded from the pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param maxTimestamp
	 *            The highest timestamp to consider (inclusive). All higher timestamps will be excluded from the pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param pageSize
	 *            The size of the page, i.e. the maximum number of elements allowed to be contained in the resulting iterator. Must be greater than zero.
	 * @param pageIndex
	 *            The index of the page to retrieve. Must not be negative.
	 * @param order
	 *            The desired ordering for the commit timestamps
	 *
	 * @return An iterator that contains the commit timestamps for the requested page. Never <code>null</code>, may be empty. If the requested page does not exist, this iterator will always be empty.
	 */
	public Iterator<Long> performGetCommitTimestampsPaged(ChronoDBTransaction tx, final long minTimestamp,
			final long maxTimestamp, final int pageSize, final int pageIndex, final Order order);

	/**
	 * Returns an iterator over commit timestamps and associated metadata in a paged fashion.
	 *
	 * <p>
	 * For example, calling {@code getCommitTimestampsPaged(10000, 100, 0, Order.DESCENDING)} will give the latest 100 commit timestamps that have occurred before timestamp 10000. Calling {@code getCommitTimestampsPaged(123456, 200, 2, Order.DESCENDING} will return 200 commit timestamps, skipping the 400 latest commit timestamps, which are smaller than 123456.
	 *
	 * <p>
	 * The {@link Entry Entries} returned by the iterator always have the commit timestamp as their first component and the metadata associated with this commit as their second component. The second component can be <code>null</code> if the commit was executed without providing metadata.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>.
	 * @param minTimestamp
	 *            The minimum timestamp to consider (inclusive). All lower timestamps will be excluded from the pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param maxTimestamp
	 *            The highest timestamp to consider. All higher timestamps will be excluded from the pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param pageSize
	 *            The size of the page, i.e. the maximum number of elements allowed to be contained in the resulting iterator. Must be greater than zero.
	 * @param pageIndex
	 *            The index of the page to retrieve. Must not be negative.
	 * @param order
	 *            The desired ordering for the commit timestamps
	 *
	 * @return An iterator that contains the commits for the requested page. Never <code>null</code>, may be empty. If the requested page does not exist, this iterator will always be empty.
	 */
	public Iterator<Entry<Long, Object>> performGetCommitMetadataPaged(final ChronoDBTransaction tx,
			final long minTimestamp, final long maxTimestamp, final int pageSize, final int pageIndex,
			final Order order);

	/**
	 * Returns pairs of commit timestamp and commit metadata which are "around" the given timestamp on the time axis.
	 *
	 * <p>
	 * By default, this method will attempt to return the closest <code>count/2</code> commits before and after the given timestamp. However, if there are not enough elements on either side, the other side will have more entries in the result list (e.g. if the request count is 10 and there are only two commits before the request timestamp, the list of commits after the request timestamp will have 8 entries instead of 5 to create a list of total length 10). In other words, the result list will always have as many entries as the request <code>count</code>, except when there are not as many commits on the store yet.
	 *
	 * @param timestamp
	 *            The request timestamp around which the commits should be centered. Must not be negative.
	 * @param count
	 *            How many commits to retrieve around the request timestamp. By default, the closest <code>count/2</code> commits will be taken on both sides of the request timestamp. Must not be negative.
	 *
	 * @return A list of pairs. The keys are commit timsetamps, the corresponding values are the commit metadata objects (which may be <code>null</code>). The list itself will never be <code>null</code>, but may be empty (if there are no commits to report). The list is sorted in descending order by timestamp.
	 */
	public List<Entry<Long, Object>> performGetCommitMetadataAround(long timestamp, int count);

	/**
	 * Returns pairs of commit timestamp and commit metadata which are strictly before the given timestamp on the time axis.
	 *
	 * <p>
	 * For example, calling {@link #performGetCommitMetadataBefore(long, int)} with a timestamp and a count of 10, this method will return the latest 10 commits (strictly) before the given request timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to investigate. Must not be negative.
	 * @param count
	 *            How many commits to retrieve before the given request timestamp. Must not be negative.
	 *
	 * @return A list of pairs. The keys are commit timsetamps, the corresponding values are the commit metadata objects (which may be <code>null</code>). The list itself will never be <code>null</code>, but may be empty (if there are no commits to report). The list is sorted in descending order by timestamp.
	 */
	public List<Entry<Long, Object>> performGetCommitMetadataBefore(long timestamp, int count);

	/**
	 * Returns pairs of commit timestamp and commit metadata which are strictly after the given timestamp on the time axis.
	 *
	 * <p>
	 * For example, calling {@link #performGetCommitMetadataAfter(long, int)} with a timestamp and a count of 10, this method will return the oldest 10 commits (strictly) after the given request timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to investigate. Must not be negative.
	 * @param count
	 *            How many commits to retrieve after the given request timestamp. Must not be negative.
	 *
	 * @return A list of pairs. The keys are commit timsetamps, the corresponding values are the commit metadata objects (which may be <code>null</code>). The list itself will never be <code>null</code>, but may be empty (if there are no commits to report). The list is sorted in descending order by timestamp.
	 */
	public List<Entry<Long, Object>> performGetCommitMetadataAfter(long timestamp, int count);

	/**
	 * Returns a list of commit timestamp which are "around" the given timestamp on the time axis.
	 *
	 * <p>
	 * By default, this method will attempt to return the closest <code>count/2</code> commits before and after the given timestamp. However, if there are not enough elements on either side, the other side will have more entries in the result list (e.g. if the request count is 10 and there are only two commits before the request timestamp, the list of commits after the request timestamp will have 8 entries instead of 5 to create a list of total length 10). In other words, the result list will always have as many entries as the request <code>count</code>, except when there are not as many commits on the store yet.
	 *
	 * @param timestamp
	 *            The request timestamp around which the commits should be centered. Must not be negative.
	 * @param count
	 *            How many commits to retrieve around the request timestamp. By default, the closest <code>count/2</code> commits will be taken on both sides of the request timestamp. Must not be negative.
	 *
	 * @return A list of timestamps. Never be <code>null</code>, but may be empty (if there are no commits to report). The list is sorted in descending order.
	 */
	public List<Long> performGetCommitTimestampsAround(long timestamp, int count);

	/**
	 * Returns a list of commit timestamps which are strictly before the given timestamp on the time axis.
	 *
	 * <p>
	 * For example, calling {@link #performGetCommitTimestampsBefore(long, int)} with a timestamp and a count of 10, this method will return the latest 10 commits (strictly) before the given request timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to investigate. Must not be negative.
	 * @param count
	 *            How many commits to retrieve before the given request timestamp. Must not be negative.
	 *
	 * @return A list of timestamps. Never be <code>null</code>, but may be empty (if there are no commits to report). The list is sorted in descending order.
	 */
	public List<Long> performGetCommitTimestampsBefore(long timestamp, int count);

	/**
	 * Returns a list of commit timestamps which are strictly after the given timestamp on the time axis.
	 *
	 * <p>
	 * For example, calling {@link #performGetCommitTimestampsAfter(long, int)} with a timestamp and a count of 10, this method will return the oldest 10 commits (strictly) after the given request timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to investigate. Must not be negative.
	 * @param count
	 *            How many commits to retrieve after the given request timestamp. Must not be negative.
	 *
	 * @return A list of timestamps. Never be <code>null</code>, but may be empty (if there are no commits to report). The list is sorted in descending order.
	 */
	public List<Long> performGetCommitTimestampsAfter(long timestamp, int count);

	/**
	 * Counts the number of commit timestamps between <code>from</code> (inclusive) and <code>to</code> (inclusive).
	 *
	 * <p>
	 * If <code>from</code> is greater than <code>to</code>, this method will always return zero.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>.
	 * @param from
	 *            The minimum timestamp to include in the search (inclusive). Must not be negative. Must be less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The maximum timestamp to include in the search (inclusive). Must not be negative. Must be less than or equal to the timestamp of this transaction.
	 *
	 * @return The number of commits that have occurred in the specified time range. May be zero, but never negative.
	 */
	public int performCountCommitTimestampsBetween(ChronoDBTransaction tx, long from, long to);

	/**
	 * Counts the total number of commit timestamps in the store.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>.
	 *
	 * @return The total number of commits in the store.
	 */
	public int performCountCommitTimestamps(ChronoDBTransaction tx);

	// =================================================================================================================
	// BRANCH LOCKING
	// =================================================================================================================

	/**
	 * Declares that a thread is about to perform a non-exclusive task that can run in parallel with other non-exclusive locking tasks.
	 *
	 * <p>
	 * This method ensures that non-exclusive operations are properly blocked when an exclusive operation is taking place.
	 *
	 * <p>
	 * This method <b>also acquires the non-exclusive lock</b> on the whole database!
	 *
	 * <p>
	 * This method must be used together with the <code>try-with-resources</code> pattern. See {@link Lockable#lockNonExclusive()} for an example.
	 *
	 * @return The object representing the lock ownership. Never <code>null</code>. Will be closed automatically by the <code>try-with-resources</code> statement.
	 */
	@Override
	public LockHolder lockNonExclusive();

	/**
	 * Declares that a thread is about to perform an exclusive task that can't run in parallel with other locking tasks.
	 *
	 * <p>
	 * This method acquires the exclusive lock on the entire database!
	 *
	 * <p>
	 * This method must be used together with the <code>try-with-resources</code> pattern. See {@link Lockable#lockExclusive()} for an example.
	 *
	 * @return The object representing the lock ownership. Never <code>null</code>. Will be closed automatically by the <code>try-with-resources</code> statement.
	 */
	@Override
	public LockHolder lockExclusive();

	/**
	 * Declares that a thread is about to perform a non-exclusive task that can run in parallel with other non-exclusive locking tasks on this branch.
	 *
	 * <p>
	 * This method <b>also acquires the non-exclusive lock</b> on the whole database!
	 *
	 * <p>
	 * This method must be used together with the <code>try-with-resources</code> pattern. See {@link Lockable#lockNonExclusive()} for an example.
	 *
	 * @return The object representing the lock ownership. Never <code>null</code>. Will be closed automatically by the <code>try-with-resources</code> statement.
	 */
	public LockHolder lockBranchExclusive();

	// =================================================================================================================
	// DUMP UTILITY
	// =================================================================================================================

	/**
	 * Returns an iterator over all entries in this data store which have timestamps less than or equal to the given timestamp.
	 *
	 * <p>
	 * This method is intended primarily for operations that perform backup work on the database. It is <b>strongly discouraged</b> to perform any kind of filtering or analysis on the returned iterator (for performance reasons); use one of the other retrieval methods provided by this class instead.
	 *
	 * <p>
	 * <b>/!\ WARNING /!\</b><br>
	 * This method will return only entries from <b>this</b> branch! Entries from the origin branch (if any) will <b>not</b> be included in the returned iterator!
	 *
	 * <p>
	 * <b>/!\ WARNING /!\</b><br>
	 * The resulting iterator <b>must</b> be {@linkplain CloseableIterator#close() closed} by the caller!
	 *
	 * @param maxTimestamp
	 *            The maximum timestamp to consider. Only entries with timestamps less than or equal to this value will be returned. Must not be negative.
	 *
	 * @return An iterator over all entries with timestamps less than or equal to the given timestamp. May be empty, but never <code>null</code>. Must be closed by the caller.
	 */
	public CloseableIterator<ChronoDBEntry> allEntriesIterator(long maxTimestamp);

	/**
	 * Directly inserts the given entries into this store, without performing any temporal consistency checks.
	 *
	 * <p>
	 * This method is intended to be used only for loading a previously stored backup, and is <b>strongly discouraged</b> for regular use. Please use transactions and commits instead to ensure temporal consistency.
	 *
	 * @param entries
	 *            The set of entries to insert. Must not be <code>null</code>. If the set is empty, this method is effectively a no-op.
	 */
	public void insertEntries(Set<ChronoDBEntry> entries);

	// =================================================================================================================
	// DEBUG METHODS
	// =================================================================================================================

	/**
	 * Executes the given debug action before the primary index update occurs.
	 *
	 * <p>
	 * This method is intended for debugging purposes only and should not be used during normal operation.
	 *
	 * @param action
	 *            The action to be executed. Must not be <code>null</code>.
	 */
	public void setDebugCallbackBeforePrimaryIndexUpdate(final Consumer<ChronoDBTransaction> action);

	/**
	 * Executes the given debug action before the secondary index update occurs.
	 *
	 * <p>
	 * This method is intended for debugging purposes only and should not be used during normal operation.
	 *
	 * @param action
	 *            The action to be executed. Must not be <code>null</code>.
	 */
	public void setDebugCallbackBeforeSecondaryIndexUpdate(final Consumer<ChronoDBTransaction> action);

	/**
	 * Executes the given debug action before the commit metadata update occurs.
	 *
	 * <p>
	 * This method is intended for debugging purposes only and should not be used during normal operation.
	 *
	 * @param action
	 *            The action to be executed. Must not be <code>null</code>.
	 */
	public void setDebugCallbackBeforeMetadataUpdate(final Consumer<ChronoDBTransaction> action);

	/**
	 * Executes the given debug action before the cache update occurs.
	 *
	 * <p>
	 * This method is intended for debugging purposes only and should not be used during normal operation.
	 *
	 * @param action
	 *            The action to be executed. Must not be <code>null</code>.
	 */
	public void setDebugCallbackBeforeCacheUpdate(final Consumer<ChronoDBTransaction> action);

	/**
	 * Executes the given debug action before the update of the {@linkplain Branch#getNow() now} timestamp occurs.
	 *
	 * <p>
	 * This method is intended for debugging purposes only and should not be used during normal operation.
	 *
	 * @param action
	 *            The action to be executed. Must not be <code>null</code>.
	 */
	public void setDebugCallbackBeforeNowTimestampUpdate(final Consumer<ChronoDBTransaction> action);

	/**
	 * Executes the given debug action before the transaction commit occurs.
	 *
	 * <p>
	 * This method is intended for debugging purposes only and should not be used during normal operation.
	 *
	 * @param action
	 *            The action to be executed. Must not be <code>null</code>.
	 */
	public void setDebugCallbackBeforeTransactionCommitted(final Consumer<ChronoDBTransaction> action);

}
