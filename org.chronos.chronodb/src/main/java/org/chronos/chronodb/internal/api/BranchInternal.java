package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.internal.impl.BranchMetadata;

/**
 * An extended version of the {@link Branch} interface.
 *
 * <p>
 * This interface and its methods are for internal use only, are subject to change and are not considered to be part of
 * the public API. Down-casting objects to internal interfaces may cause application code to become incompatible with
 * future releases, and is therefore strongly discouraged.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface BranchInternal extends Branch {

	/**
	 * Returns the metadata associated with this branch.
	 *
	 * @return The metadata. Never <code>null</code>.
	 */
	public BranchMetadata getMetadata();

	/**
	 * Returns the {@link TemporalKeyValueStore} for this branch.
	 *
	 * <p>
	 * This method is <b>not</b> considered to be part of the public API. API users should never call this method
	 * directly, as it is intended for internal use only.
	 *
	 * @return The temporal key-value store that represents this branch. Never <code>null</code>.
	 */
	public TemporalKeyValueStore getTemporalKeyValueStore();

	/**
	 * Associates the given {@link TemporalKeyValueStore} with this branch.
	 *
	 * @param tkvs
	 *            The temporal key value store to use in this branch. Must not be <code>null</code>. Must not be owned
	 *            by any other branch.
	 */
	public void setTemporalKeyValueStore(final TemporalKeyValueStore tkvs);

}
