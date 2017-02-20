package org.chronos.chronodb.internal.api;

import java.util.Iterator;
import java.util.Map.Entry;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.internal.util.ChronosBackend;

/**
 * A {@link CommitMetadataStore} is a store in the {@link ChronosBackend} that contains metadata objects for commit
 * operations.
 *
 * <p>
 * Any commit may have a metadata object attached. For details, please refer to
 * {@link ChronoDBTransaction#commit(Object)}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface CommitMetadataStore {

	/**
	 * Returns an iterator over all timestamps where commits have occurred, bounded between <code>from</code> and
	 * <code>to</code>, in descending order.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative.
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
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative.
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
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative.
	 *
	 * @return An iterator over the commits in the given time range in descending order. The containd entries have the
	 *         timestamp as the first component and the associated metadata as their second component (which may be
	 *         <code>null</code>). May be empty, but never <code>null</code>.
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
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative.
	 * @param order
	 *            The order of the returned commits. Must not be <code>null</code>.
	 *
	 * @return An iterator over the commits in the given time range. The containd entries have the timestamp as the
	 *         first component and the associated metadata as their second component (which may be <code>null</code>).
	 *         May be empty, but never <code>null</code>.
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
	 *            pagination.
	 * @param maxTimestamp
	 *            The highest timestamp to consider (inclusive). All higher timestamps will be excluded from the
	 *            pagination.
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
	 *            pagination.
	 * @param maxTimestamp
	 *            The highest timestamp to consider. All higher timestamps will be excluded from the pagination.
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
	 * Counts the number of commit timestamps between <code>from</code> (inclusive) and <code>to</code> (inclusive).
	 *
	 * <p>
	 * If <code>from</code> is greater than <code>to</code>, this method will always return zero.
	 *
	 * @param from
	 *            The minimum timestamp to include in the search (inclusive). Must not be negative.
	 * @param to
	 *            The maximum timestamp to include in the search (inclusive). Must not be negative.
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
	 * Puts the given commit metadata into the store and associates it with the given commit timestamp.
	 *
	 * @param commitTimestamp
	 *            The commit timestamp to associate the metadata with. Must not be negative.
	 * @param commitMetadata
	 *            The commit metadata to associate with the timestamp. May be <code>null</code>.
	 */
	public void put(long commitTimestamp, Object commitMetadata);

	/**
	 * Returns the commit metadata for the commit that occurred at the given timestamp.
	 *
	 * @param commitTimestamp
	 *            The commit timestamp to get the metadata for. Must not be negative.
	 * @return The commit metadata object associated with the commit at the given timestamp. May be <code>null</code> if
	 *         no metadata was given for the commit.
	 */
	public Object get(long commitTimestamp);

	/**
	 * Rolls back the contents of the store to the given timestamp.
	 *
	 * <p>
	 * Any data associated with timestamps strictly larger than the given one will be removed from the store.
	 *
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative.
	 */
	public void rollbackToTimestamp(long timestamp);

	/**
	 * Returns the {@link Branch} to which this store belongs.
	 *
	 * @return The owning branch. Never <code>null</code>.
	 */
	public Branch getOwningBranch();

}
