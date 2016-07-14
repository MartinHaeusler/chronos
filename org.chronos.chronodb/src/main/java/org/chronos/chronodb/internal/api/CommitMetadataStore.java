package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBTransaction;
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
 *
 */
public interface CommitMetadataStore {

	/**
	 * Puts the given commit metadata into the store and associates it with the given commit timestamp.
	 *
	 * @param commitTimestamp
	 *            The commit timestamp to associate the metadata with. Must not be negative.
	 * @param commitMetadata
	 *            The commit metadata to associate with the timestamp. Must not be <code>null</code>.
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
