package org.chronos.chronodb.api;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.chronos.chronodb.api.builder.query.QueryBuilderFinalizer;
import org.chronos.chronodb.api.builder.query.QueryBuilderStarter;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.api.exceptions.TransactionIsReadOnlyException;
import org.chronos.chronodb.api.exceptions.UnknownKeyspaceException;
import org.chronos.chronodb.api.exceptions.ValueTypeMismatchException;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;

/**
 * A {@link ChronoDBTransaction} encapsulates a set of queries and/or change operations on a {@link ChronoDB}.
 *
 * <p>
 * Instances of this interface are produced by a {@link TransactionSource}. API users should make use of the methods of
 * {@link ChronoDB}, which implements TransactionSource.
 *
 * <p>
 * Instances of this class are in general not guaranteed to be thread-safe, and should not be shared among threads.
 *
 * <p>
 * Note that opening a ChronoTransaction does not imply any further obligations. In particular, ChronoTransactions do
 * not have to be closed or terminated in any way.
 *
 * <p>
 * Changes made to the transaction will not be persisted immediately. They will be kept in-memory until
 * {@link #commit()} is invoked. Calling {@link #rollback()} will clear this in-memory cache.
 *
 * <p>
 * Users may continue to use a {@link ChronoDBTransaction} after calling {@link #commit()} or {@link #rollback()} on it.
 * It will behave as if it had been newly instantiated.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBTransaction {

	// =====================================================================================================================
	// TRANSACTION METADATA
	// =====================================================================================================================

	/**
	 * Returns the timestamp on which this transaction operates.
	 *
	 * @return The timestamp, which is always a number greater than (or equal to) zero.
	 */
	public long getTimestamp();

	/**
	 * Returns an identifier for the branch on which this transaction operates.
	 *
	 * @return The branch identifier
	 */
	public String getBranchName();

	// =====================================================================================================================
	// DATA RETRIEVAL
	// =====================================================================================================================

	/**
	 * Returns the value of the given key in the <i>default</i> keyspace, at the timestamp of this transaction.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * // note the automatic cast to the expected type
	 * String value = tx.get(&quot;Hello&quot;);
	 * </pre>
	 *
	 * @param key
	 *            The key to get the value for. Must not be <code>null</code>.
	 *
	 * @return The value associated with the given key, cast to the given generic argument. Returns <code>null</code> if
	 *         there is no value for the given key.
	 *
	 * @throws ValueTypeMismatchException
	 *             Thrown if the stored value cannot be cast to the expected type argument.
	 */
	public <T> T get(String key) throws ValueTypeMismatchException;

	/**
	 * Returns the value of the given key in the given keyspace, at the timestamp of this transaction.
	 *
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * // note the automatic cast to the expected type
	 * String value = tx.get(&quot;MyKeyspace&quot;, &quot;Hello&quot;);
	 * </pre>
	 *
	 * @param keyspaceName
	 *            The name of the keyspace to search in. Must not be <code>null</code>.
	 * @param key
	 *            The key to get the value for. Must not be <code>null</code>.
	 *
	 * @return The value associated with the given key, cast to the given generic argument. Returns <code>null</code> if
	 *         there is no value for the given key.
	 *
	 * @throws ValueTypeMismatchException
	 *             Thrown if the stored value cannot be cast to the expected type argument.
	 *
	 * @throws UnknownKeyspaceException
	 *             Thrown if the specified keyspace name does not refer to an existing keyspace.
	 */
	public <T> T get(String keyspaceName, String key) throws ValueTypeMismatchException, UnknownKeyspaceException;

	/**
	 * Checks if there is a value for the given key in the <i>default</i> keyspace, at the timestamp of this
	 * transaction.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * boolean keyExists = tx.exists("MyKey")
	 * </pre>
	 *
	 * @param key
	 *            The key to check the existence of a value for. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if there is a value for the given key, otherwise <code>false</code>.
	 */
	public boolean exists(String key);

	/**
	 * Checks if there is a value for the given key in the given keyspace, at the timestamp of this transaction.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * boolean keyExists = tx.exists("MyKeyspace", "MyKey")
	 * </pre>
	 *
	 * @param keyspaceName
	 *            The name of the keyspace to search in. Must not be <code>null</code>.
	 * @param key
	 *            The key to check the existence of a value for. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if there is a value for the given key, otherwise <code>false</code>.
	 */
	public boolean exists(String keyspaceName, String key);

	/**
	 * Returns the set of all keys present in the <i>default</i> keyspace at the timestamp of this transaction.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * for (String key : tx.keySet()) {
	 * 	tx.exists(key); // is always true
	 * }
	 * </pre>
	 *
	 * @return The key set. May be empty, but never <code>null</code>.
	 */
	public Set<String> keySet();

	/**
	 * Returns the set of all keys present in the given keyspace at the timestamp of this transaction.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * for (String key : tx.keySet(&quot;MyKeyspace&quot;)) {
	 * 	tx.exists(key); // is always true
	 * }
	 * </pre>
	 *
	 * @param keyspaceName
	 *            The name of the keyspace to scan. Must not be <code>null</code>.
	 *
	 * @return The key set. May be empty, but never <code>null</code>.
	 */
	public Set<String> keySet(String keyspaceName);

	/**
	 * Returns the timestamps in the past at which the value for the given key has changed in the <i>default</i>
	 * keyspace, up to the timestamp of this transaction.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * Iterator&lt;Long&gt; iterator = tx.history(&quot;MyKey&quot;);
	 * while (iterator.hasNext()) {
	 * 	long timestamp = iterator.next();
	 * 	Object pastValue = chronoDB.tx(timestamp).get(&quot;MyKey&quot;);
	 * 	String msg = &quot;At timestamp &quot; + timestamp + &quot;, the value of 'MyKey' was changed to '&quot; + pastValue + &quot;'&quot;;
	 * 	System.out.println(msg);
	 * }
	 * </pre>
	 *
	 * @param key
	 *            The key to get the history for. Must not be <code>null</code>.
	 *
	 * @return An iterator over the history timestamps. May be empty, but never <code>null</code>. The exact ordering of
	 *         the returned keys is up to the implementation. Implementations should try to return the timestamps in
	 *         descending order (highest first).
	 */
	public Iterator<Long> history(String key);

	/**
	 * Returns the timestamps in the past at which the value for the given key has changed in the given keyspace, up to
	 * the timestamp of this transaction.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * Iterator&lt;Long&gt; iterator = tx.history(&quot;MyKeyspace&quot;, &quot;MyKey&quot;);
	 * while (iterator.hasNext()) {
	 * 	long timestamp = iterator.next();
	 * 	Object pastValue = chronoDB.tx(timestamp).get(&quot;MyKey&quot;);
	 * 	String msg = &quot;At timestamp &quot; + timestamp + &quot;, the value of 'MyKey' was changed to '&quot; + pastValue + &quot;'&quot;;
	 * 	System.out.println(msg);
	 * }
	 * </pre>
	 *
	 * @param key
	 *            The key to get the history for. Must not be <code>null</code>.
	 * @param keyspaceName
	 *            The name of the keyspace to scan. Must not be <code>null</code>.
	 *
	 * @return An iterator over the history timestamps. May be empty, but never <code>null</code>. The exact ordering of
	 *         the returned keys is up to the implementation. Implementations should try to return the timestamps in
	 *         descending order (highest first).
	 */
	public Iterator<Long> history(String keyspaceName, String key);

	/**
	 * Returns an iterator over the modified keys in the given timestamp range.
	 *
	 * <p>
	 * Please note that <code>timestampLowerBound</code> and <code>timestampUpperBound</code> must not be larger than
	 * the timestamp of this transaction, i.e. you can only look for timestamp ranges in the past with this method.
	 *
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
	public Iterator<TemporalKey> getModificationsInKeyspaceBetween(String keyspace, long timestampLowerBound,
			long timestampUpperBound);

	/**
	 * Returns the metadata object stored alongside the commit at the given timestamp.
	 *
	 * <p>
	 * It is important that the given timestamp matches the commit timestamp <b>exactly</b>. Commit timestamps can be
	 * retrieved for example via {@link #getModificationsInKeyspaceBetween(String, long, long)}.
	 *
	 * @param commitTimestamp
	 *            The commit timestamp to get the metadata object for. Must match the commit timestamp exactly. Must not
	 *            be negative. Must be less than or equal to the timestamp of this transaction (i.e. must be in the
	 *            past).
	 *
	 * @return The object stored alongside that commit. Will be <code>null</code> for all timestamps that are not
	 *         associated with a commit. May also be <code>null</code> in cases where no metadata object was given for
	 *         the commit by the user.
	 */
	public Object getCommitMetadata(long commitTimestamp);

	/**
	 * Returns an iterator over all timestamps where commits have occurred, bounded between <code>from</code> and
	 * <code>to</code>, in descending order.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @return The iterator over the commit timestamps in the given time range, in descending order. May be empty, but
	 *         never <code>null</code>.
	 */
	public default Iterator<Long> getCommitTimestampsBetween(final long from, final long to) {
		return this.getCommitTimestampsBetween(from, to, Order.DESCENDING);
	}

	/**
	 * Returns an iterator over all timestamps where commits have occurred, bounded between <code>from</code> and
	 * <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned timestamps. Must not be <code>null</code>.
	 * @return The iterator over the commit timestamps in the given time range. May be empty, but never
	 *         <code>null</code>.
	 */
	public Iterator<Long> getCommitTimestampsBetween(long from, long to, Order order);

	/**
	 * Returns an iterator over the entries of commit timestamp and associated metadata, bounded between
	 * <code>from</code> and <code>to</code>, in descending order.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * <p>
	 * Please keep in mind that some commits may not have any metadata attached. In this case, the
	 * {@linkplain Entry#getValue() value} component of the {@link Entry} will be set to <code>null</code>.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 *
	 * @return An iterator over the commits in the given time range in descending order. The contained entries have the
	 *         timestamp as the {@linkplain Entry#getKey() key} component and the associated metadata as their
	 *         {@linkplain Entry#getValue() value} component (which may be <code>null</code>). May be empty, but never
	 *         <code>null</code>.
	 */
	public default Iterator<Entry<Long, Object>> getCommitMetadataBetween(final long from, final long to) {
		return this.getCommitMetadataBetween(from, to, Order.DESCENDING);
	}

	/**
	 * Returns an iterator over the entries of commit timestamp and associated metadata, bounded between
	 * <code>from</code> and <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * <p>
	 * Please keep in mind that some commits may not have any metadata attached. In this case, the
	 * {@linkplain Entry#getValue() value} component of the {@link Entry} will be set to <code>null</code>.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned commits. Must not be <code>null</code>.
	 *
	 * @return An iterator over the commits in the given time range. The contained entries have the timestamp as the
	 *         {@linkplain Entry#getKey() key} component and the associated metadata as their
	 *         {@linkplain Entry#getValue() value} component (which may be <code>null</code>). May be empty, but never
	 *         <code>null</code>.
	 */
	public Iterator<Entry<Long, Object>> getCommitMetadataBetween(long from, long to, Order order);

	/**
	 * Returns an iterator over commit timestamps in a paged fashion.
	 *
	 * <p>
	 * For example, calling {@code getCommitTimestampsPaged(10000, 100, 0, Order.DESCENDING)} will give the latest 100
	 * commit timestamps that have occurred before timestamp 10000. Calling
	 * {@code getCommitTimestampsPaged(123456, 200, 2, Order.DESCENDING} will return 200 commit timestamps, skipping the
	 * 400 latest commit timestamps, which are smaller than 123456.
	 *
	 * @param minTimestamp
	 *            The minimum timestamp to consider (inclusive). All lower timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param maxTimestamp
	 *            The highest timestamp to consider (inclusive). All higher timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param pageSize
	 *            The size of the page, i.e. the maximum number of elements allowed to be contained in the resulting
	 *            iterator. Must be greater than zero.
	 * @param pageIndex
	 *            The index of the page to retrieve. Must not be negative.
	 * @param order
	 *            The desired ordering for the commit timestamps
	 *
	 * @return An iterator that contains the commit timestamps for the requested page. Never <code>null</code>, may be
	 *         empty. If the requested page does not exist, this iterator will always be empty.
	 */
	public Iterator<Long> getCommitTimestampsPaged(final long minTimestamp, final long maxTimestamp, final int pageSize,
			final int pageIndex, final Order order);

	/**
	 * Returns an iterator over commit timestamps and associated metadata in a paged fashion.
	 *
	 * <p>
	 * For example, calling {@code getCommitTimestampsPaged(10000, 100, 0, Order.DESCENDING)} will give the latest 100
	 * commit timestamps that have occurred before timestamp 10000. Calling
	 * {@code getCommitTimestampsPaged(123456, 200, 2, Order.DESCENDING} will return 200 commit timestamps, skipping the
	 * 400 latest commit timestamps, which are smaller than 123456.
	 *
	 * <p>
	 * The {@link Entry Entries} returned by the iterator always have the commit timestamp as their first component and
	 * the metadata associated with this commit as their second component. The second component can be <code>null</code>
	 * if the commit was executed without providing metadata.
	 *
	 * @param minTimestamp
	 *            The minimum timestamp to consider (inclusive). All lower timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param maxTimestamp
	 *            The highest timestamp to consider. All higher timestamps will be excluded from the pagination. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param pageSize
	 *            The size of the page, i.e. the maximum number of elements allowed to be contained in the resulting
	 *            iterator. Must be greater than zero.
	 * @param pageIndex
	 *            The index of the page to retrieve. Must not be negative.
	 * @param order
	 *            The desired ordering for the commit timestamps
	 *
	 * @return An iterator that contains the commits for the requested page. Never <code>null</code>, may be empty. If
	 *         the requested page does not exist, this iterator will always be empty.
	 */
	public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final long minTimestamp, final long maxTimestamp,
			final int pageSize, final int pageIndex, final Order order);

	/**
	 * Returns pairs of commit timestamp and commit metadata which are "around" the given timestamp on the time axis.
	 *
	 * <p>
	 * By default, this method will attempt to return the closest <code>count/2</code> commits before and after the
	 * given timestamp. However, if there are not enough elements on either side, the other side will have more entries
	 * in the result list (e.g. if the request count is 10 and there are only two commits before the request timestamp,
	 * the list of commits after the request timestamp will have 8 entries instead of 5 to create a list of total length
	 * 10). In other words, the result list will always have as many entries as the request <code>count</code>, except
	 * when there are not as many commits on the store yet.
	 *
	 * @param timestamp
	 *            The request timestamp around which the commits should be centered. Must not be negative.
	 * @param count
	 *            How many commits to retrieve around the request timestamp. By default, the closest
	 *            <code>count/2</code> commits will be taken on both sides of the request timestamp. Must not be
	 *            negative.
	 *
	 * @return A list of pairs. The keys are commit timsetamps, the corresponding values are the commit metadata objects
	 *         (which may be <code>null</code>). The list itself will never be <code>null</code>, but may be empty (if
	 *         there are no commits to report). The list is sorted in descending order by timestamps.
	 */
	public List<Entry<Long, Object>> getCommitMetadataAround(final long timestamp, final int count);

	/**
	 * Returns pairs of commit timestamp and commit metadata which are strictly before the given timestamp on the time
	 * axis.
	 *
	 * <p>
	 * For example, calling {@link #getCommitMetadataBefore(long, int)} with a timestamp and a count of 10, this method
	 * will return the latest 10 commits (strictly) before the given request timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to investigate. Must not be negative.
	 * @param count
	 *            How many commits to retrieve before the given request timestamp. Must not be negative.
	 *
	 * @return A list of pairs. The keys are commit timsetamps, the corresponding values are the commit metadata objects
	 *         (which may be <code>null</code>). The list itself will never be <code>null</code>, but may be empty (if
	 *         there are no commits to report). The list is sorted in descending order by timestamps.
	 */
	public List<Entry<Long, Object>> getCommitMetadataBefore(final long timestamp, final int count);

	/**
	 * Returns pairs of commit timestamp and commit metadata which are strictly after the given timestamp on the time
	 * axis.
	 *
	 * <p>
	 * For example, calling {@link #getCommitMetadataAfter(long, int)} with a timestamp and a count of 10, this method
	 * will return the oldest 10 commits (strictly) after the given request timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to investigate. Must not be negative.
	 * @param count
	 *            How many commits to retrieve after the given request timestamp. Must not be negative.
	 *
	 * @return A list of pairs. The keys are commit timsetamps, the corresponding values are the commit metadata objects
	 *         (which may be <code>null</code>). The list itself will never be <code>null</code>, but may be empty (if
	 *         there are no commits to report). The list is sorted in descending order by timestamps.
	 */
	public List<Entry<Long, Object>> getCommitMetadataAfter(final long timestamp, final int count);

	/**
	 * Returns a list of commit timestamp which are "around" the given timestamp on the time axis.
	 *
	 * <p>
	 * By default, this method will attempt to return the closest <code>count/2</code> commits before and after the
	 * given timestamp. However, if there are not enough elements on either side, the other side will have more entries
	 * in the result list (e.g. if the request count is 10 and there are only two commits before the request timestamp,
	 * the list of commits after the request timestamp will have 8 entries instead of 5 to create a list of total length
	 * 10). In other words, the result list will always have as many entries as the request <code>count</code>, except
	 * when there are not as many commits on the store yet.
	 *
	 * @param timestamp
	 *            The request timestamp around which the commits should be centered. Must not be negative.
	 * @param count
	 *            How many commits to retrieve around the request timestamp. By default, the closest
	 *            <code>count/2</code> commits will be taken on both sides of the request timestamp. Must not be
	 *            negative.
	 *
	 * @return A list of timestamps. Never be <code>null</code>, but may be empty (if there are no commits to report).
	 *         The list is sorted in descending order.
	 */
	public List<Long> getCommitTimestampsAround(final long timestamp, final int count);

	/**
	 * Returns a list of commit timestamps which are strictly before the given timestamp on the time axis.
	 *
	 * <p>
	 * For example, calling {@link #getCommitTimestampsBefore(long, int)} with a timestamp and a count of 10, this
	 * method will return the latest 10 commits (strictly) before the given request timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to investigate. Must not be negative.
	 * @param count
	 *            How many commits to retrieve before the given request timestamp. Must not be negative.
	 *
	 * @return A list of timestamps. Never be <code>null</code>, but may be empty (if there are no commits to report).
	 *         The list is sorted in descending order.
	 */
	public List<Long> getCommitTimestampsBefore(final long timestamp, final int count);

	/**
	 * Returns a list of commit timestamps which are strictly after the given timestamp on the time axis.
	 *
	 * <p>
	 * For example, calling {@link #getCommitTimestampsAfter(long, int)} with a timestamp and a count of 10, this method
	 * will return the oldest 10 commits (strictly) after the given request timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to investigate. Must not be negative.
	 * @param count
	 *            How many commits to retrieve after the given request timestamp. Must not be negative.
	 *
	 * @return A list of timestamps. Never be <code>null</code>, but may be empty (if there are no commits to report).
	 *         The list is sorted in descending order.
	 */
	public List<Long> getCommitTimestampsAfter(final long timestamp, final int count);

	/**
	 * Counts the number of commit timestamps between <code>from</code> (inclusive) and <code>to</code> (inclusive).
	 *
	 * <p>
	 * If <code>from</code> is greater than <code>to</code>, this method will always return zero.
	 *
	 * @param from
	 *            The minimum timestamp to include in the search (inclusive). Must not be negative. Must be less than or
	 *            equal to the timestamp of this transaction.
	 * @param to
	 *            The maximum timestamp to include in the search (inclusive). Must not be negative. Must be less than or
	 *            equal to the timestamp of this transaction.
	 *
	 * @return The number of commits that have occurred in the specified time range. May be zero, but never negative.
	 */
	public int countCommitTimestampsBetween(long from, long to);

	/**
	 * Counts the total number of commit timestamps in the store.
	 *
	 * @return The total number of commits in the store.
	 */
	public int countCommitTimestamps();

	/**
	 * Returns the set of keyspace names currently used by the database on this branch.
	 *
	 * @return An unmodifiable set of keyspace names. May be empty, but never <code>null</code>.
	 */
	public Set<String> keyspaces();

	/**
	 * Returns a {@link QueryBuilderStarter} that allows for fluent design of queries.
	 *
	 * <p>
	 * For usage examples, please check the individual methods of the QueryBuilder.
	 *
	 * @return A query builder.
	 */
	public QueryBuilderStarter find();

	/**
	 * Returns a {@link QueryBuilderFinalizer} that allows to execute the given query in various ways.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * // prepare a query
	 * ChronoDBQuery query = tx.find().inDefaultKeyspace().where("name").contains("Simon").toQuery();
	 * // this query can now be executed anywhere, even on another transaction!
	 * Iterator&lt;Object&gt; values = tx.find(query).getValues();
	 * </pre>
	 *
	 * @param query
	 *            The prebuilt query to execute. Must not be <code>null</code>.
	 *
	 * @return A finalizer builder for the query.
	 */
	public QueryBuilderFinalizer find(ChronoDBQuery query);

	// =====================================================================================================================
	// DATA MODIFICATION
	// =====================================================================================================================

	/**
	 * Sets the given key in the <i>default</i> keyspace to the given value.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.put(&quot;Pi&quot;, 3.1415);
	 * </pre>
	 *
	 * Note that the {@link #get(String)} method and its overloads (as well as {@link #find()}) will <b>not</b> return
	 * the values assigned by this method, until {@link #commit()} is called.
	 *
	 * @param key
	 *            The key to modify. Must not be <code>null</code>.
	 * @param value
	 *            The new value to assign to the given key. Must not be <code>null</code>.
	 */
	public void put(String key, Object value);

	/**
	 * Sets the given key in the <i>default</i> keyspace to the given value.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.put(&quot;Pi&quot;, 3.1415);
	 * </pre>
	 *
	 * Note that the {@link #get(String)} method and its overloads (as well as {@link #find()}) will <b>not</b> return
	 * the values assigned by this method, until {@link #commit()} is called.
	 *
	 * @param key
	 *            The key to modify. Must not be <code>null</code>.
	 * @param value
	 *            The new value to assign to the given key. Must not be <code>null</code>.
	 * @param options
	 *            The options to apply. May be empty.
	 */
	public void put(String key, Object value, PutOption... options);

	/**
	 * Sets the given key in the given keyspace to the given value.
	 *
	 * <p>
	 * If the keyspace with the given name does not yet exist, it will be created on-the-fly when this change is
	 * successfully committed.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.put(&quot;MyKeyspace&quot;, &quot;Pi&quot;, 3.1415);
	 * </pre>
	 *
	 * Note that the {@link #get(String)} method and its overloads (as well as {@link #find()}) will <b>not</b> return
	 * the values assigned by this method, until {@link #commit()} is called.
	 *
	 *
	 * @param keyspaceName
	 *            The name of the keyspace to modify. Must not be <code>null</code>.
	 * @param key
	 *            The key to modify. Must not be <code>null</code>.
	 * @param value
	 *            The new value to assign to the given key. Must not be <code>null</code>.
	 */
	public void put(String keyspaceName, String key, Object value);

	/**
	 * Sets the given key in the given keyspace to the given value.
	 *
	 * <p>
	 * If the keyspace with the given name does not yet exist, it will be created on-the-fly when this change is
	 * successfully committed.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.put(&quot;MyKeyspace&quot;, &quot;Pi&quot;, 3.1415, PutOption.NONE);
	 * </pre>
	 *
	 * Note that the {@link #get(String)} method and its overloads (as well as {@link #find()}) will <b>not</b> return
	 * the values assigned by this method, until {@link #commit()} is called.
	 *
	 *
	 * @param keyspaceName
	 *            The name of the keyspace to modify. Must not be <code>null</code>.
	 * @param key
	 *            The key to modify. Must not be <code>null</code>.
	 * @param value
	 *            The new value to assign to the given key. Must not be <code>null</code>.
	 * @param options
	 *            The options to apply. May be empty.
	 */
	public void put(String keyspaceName, String key, Object value, PutOption... options);

	/**
	 * Removes the given key from the <i>default</i> keyspace.
	 *
	 * <p>
	 * If the key given by the parameter does not exist in the keyspace, this method does nothing.
	 *
	 * @param key
	 *            The key to remove. Must not be <code>null</code>.
	 */
	public void remove(String key);

	/**
	 * Removes the given key from the given keyspace.
	 *
	 * <p>
	 * If the key given by the parameter does not exist in the keyspace, this method does nothing.
	 *
	 * @param keyspaceName
	 *            The name of the keyspace to modify. Must not be <code>null</code>.
	 * @param key
	 *            The key to remove. Must not be <code>null</code>.
	 */
	public void remove(String keyspaceName, String key);

	// =====================================================================================================================
	// TRANSACTION CONTROL
	// =====================================================================================================================

	/**
	 * Commits the changes made to this transaction to the {@link ChronoDB} instance.
	 *
	 * <p>
	 * This is an <i>atomic</i>, <i>consistent</i>, <i>isolated</i> and <i>durable</i> operation:
	 * <ul>
	 * <li>Either the entire change set is committed, or nothing is committed.
	 * <li>Changes to commit will be validated against certain constraints, which may cause the commit to fail to ensure
	 * database consistency.
	 * <li>No other transactions will see partial results of the commit.
	 * <li>Once this method returns successfully, the results of the commit are persisted and durable.
	 * </ul>
	 *
	 * <p>
	 * After calling {@link #commit()}, the transaction will receive a new timestamp, i.e. it will be forwarded in time
	 * to the time after the commit it just performed. Clients may continue to use transactions after calling
	 * <code>commit()</code> on them.
	 *
	 * @throws ChronoDBCommitException
	 *             Thrown if a commit failed due to consistency violations.
	 */
	public void commit() throws ChronoDBCommitException;

	/**
	 * Commits the changes made to this transaction to the {@link ChronoDB} instance.
	 *
	 * <p>
	 * This is an <i>atomic</i>, <i>consistent</i>, <i>isolated</i> and <i>durable</i> operation:
	 * <ul>
	 * <li>Either the entire change set is committed, or nothing is committed.
	 * <li>Changes to commit will be validated against certain constraints, which may cause the commit to fail to ensure
	 * database consistency.
	 * <li>No other transactions will see partial results of the commit.
	 * <li>Once this method returns successfully, the results of the commit are persisted and durable.
	 * </ul>
	 *
	 * <p>
	 * After calling {@link #commit()}, the transaction will receive a new timestamp, i.e. it will be forwarded in time
	 * to the time after the commit it just performed. Clients may continue to use transactions after calling
	 * <code>commit()</code> on them.
	 *
	 * @param commitMetadata
	 *            The metadata object to store alongside this commit. May be <code>null</code>. Will be serialized using
	 *            the {@link SerializationManager} associated with this {@link ChronoDB} instance. Using primitives,
	 *            collections of primitives or maps of primitives is recommended.
	 *
	 * @throws ChronoDBCommitException
	 *             Thrown if a commit failed due to consistency violations.
	 */
	public void commit(Object commitMetadata) throws ChronoDBCommitException;

	/**
	 * Performs an incremental commit on this transaction.
	 *
	 * <p>
	 * Incremental commits can be used to insert large batches of data into a {@link ChronoDB} instance. Their advantage
	 * over normal transactions is that they do not require the entire data to be contained in main memory before
	 * writing it to the storage backend.
	 *
	 * <p>
	 * Recommended usage of this method:
	 *
	 * <pre>
	 * ChronoDBTransaction tx = db.tx();
	 * try {
	 * 	// ... do some heavy work
	 * 	tx.commitIncremental();
	 * 	// ... more work...
	 * 	tx.commitIncremental();
	 * 	// ... more work...
	 * 	// ... and finally, accept the changes and make them visible to others
	 * 	tx.commit();
	 * } finally {
	 * 	// make absolutely sure that the incremental process is terminated in case of error
	 * 	tx.rollback();
	 * }
	 * </pre>
	 *
	 * <p>
	 * Using an incremental commit implies all of the following facts:
	 * <ul>
	 * <li>Only one incremental commit process may be active on any given {@link ChronoDB} instance at any point in
	 * time. Attempting to have multiple incremental commit processes on the same {@link ChronoDB} instance will result
	 * in a {@link ChronoDBCommitException} on all processes, except for the first process.
	 * <li>While an incremental commit process is active, no regular commits on other transactions can be accepted.
	 * <li>Other transactions may continue to read data while an incremental commit process is active, but they cannot
	 * see the changes made by incremental commits.
	 * <li>An incremental commit process consists of several calls to {@link #commitIncremental()}, and is terminated
	 * either by a full-fledged {@link #commit()}, or by a {@link #rollback()}.
	 * <li>It is the responsibility of the caller to ensure that either {@link #commit()} or {@link #rollback()} are
	 * called after the initial {@link #commitIncremental()} was called. Failure to do so will result in this
	 * {@link ChronoDB} instance no longer accepting any commits.
	 * <li>After the terminating {@link #commit()}, the changes will be visible to other transactions, provided that
	 * they use an appropriate timestamp.
	 * <li>The timestamp of all changes in an incremental commit process is the timestamp of the first
	 * {@link #commitIncremental()} invocation.
	 * <li>In contrast to regular transactions, several calls to {@link #commitIncremental()} may modify the same data.
	 * Overwrites within the same incremental commit process are <b>not</b> tracked by the versioning system, and follow
	 * the "last writer wins" principle.
	 * <li>In the history of any given key, an incremental commit process appears as a single, large commit. In
	 * particular, it is not possible to select a point in time where only parts of the incremental commit process were
	 * applied.
	 * <li>A call to {@link #commitIncremental()} does not update the {@link TemporalKeyValueStore#getNow()} timestamp.
	 * Only the terminating call to {@link #commit()} updates this timestamp.
	 * <li>The timestamp of the transaction executing the incremental commit will be increased after each call to
	 * {@link #commitIncremental()} in order to allow the transaction to read the data it has written in the incremental
	 * update. If the incremental commit process fails at any point, this timestamp will be reverted to the original
	 * timestamp of the transaction before the incremental commit process started.
	 * <li>Durability of the changes made by {@link #commitIncremental()} can only be guaranteed by {@link ChronoDB} if
	 * the terminating call to {@link #commit()} is successful. Any other changes may be lost in case of errors.
	 * <li>If the JVM process is terminated during an incremental commit process, and the terminating call to
	 * {@link #commit()} has not yet been completed successfully, any data stored by that process will be lost and
	 * rolled back on the next startup.
	 * </ul>
	 *
	 * @throws ChronoDBCommitException
	 *             Thrown if the commit fails. If this exception is thrown, the entire incremental commit process is
	 *             aborted, and any changes to the data made by that process will be rolled back.
	 */
	public void commitIncremental() throws ChronoDBCommitException;

	/**
	 * Performs a rollback on this transaction, clearing all changes made to it.
	 *
	 * <p>
	 * This transaction may be used after calling rollback, in contrast to when calling {@link #commit()}.
	 */
	public void rollback();

	// =====================================================================================================================
	// INTERNALS
	// =====================================================================================================================

	/**
	 * Returns the change set that contains the pending changes which will be stored upon calling {@link #commit()}.
	 *
	 * @return An unmodifiable view on the change set. May be empty, but never <code>null</code>.
	 */
	public Collection<ChangeSetEntry> getChangeSet();

	/**
	 * Returns the configuration of this transaction.
	 *
	 * @return The transaction configuration. Never <code>null</code>.
	 */
	public Configuration getConfiguration();

	/**
	 * This interface represents the configuration of a single transaction.
	 *
	 * <p>
	 * Clients must not implement this interface, and must not attempt to instantiate implementing classes. Instances of
	 * this class are forwarded to the client via {@link ChronoDBTransaction#getConfiguration()} for read-only purposes.
	 *
	 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
	 *
	 */
	public interface Configuration {

		/**
		 * Checks if this transaction is thread-safe, i.e. may be shared among multiple threads.
		 *
		 * @return <code>true</code> if the transaction is thread-safe, otherwise <code>false</code>.
		 */
		public boolean isThreadSafe();

		/**
		 * Checks if blind overwrite protection is enabled on this transaction.
		 *
		 * @return <code>true</code> if blind overwrite protection is enabled, otherwise <code>false</code>.
		 */
		public boolean isBlindOverwriteProtectionEnabled();

		/**
		 * Returns the {@link DuplicateVersionEliminationMode} associated with this transaction.
		 *
		 * @return The duplicate version elimination mode. Never <code>null</code>.
		 */
		public DuplicateVersionEliminationMode getDuplicateVersionEliminationMode();

		/**
		 * Checks if this transaction is read-only.
		 *
		 * <p>
		 * Read-only transactions throw a {@link TransactionIsReadOnlyException} if a modifying method, such as
		 * {@link ChronoDBTransaction#put(String, Object)}, is called.
		 *
		 * @return <code>true</code> if this transaction is read-only, otherwise <code>false</code>.
		 */
		public boolean isReadOnly();

	}
}
