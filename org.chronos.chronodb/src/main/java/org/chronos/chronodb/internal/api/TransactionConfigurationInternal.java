package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.ChronoDBTransaction;

/**
 * An extended version of {@link org.chronos.chronodb.api.ChronoDBTransaction.Configuration}.
 *
 * <p>
 * This interface and its methods are for internal use only, are subject to change and are not considered to be part of
 * the public API. Down-casting objects to internal interfaces may cause application code to become incompatible with
 * future releases, and is therefore strongly discouraged.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface TransactionConfigurationInternal extends ChronoDBTransaction.Configuration {

	/**
	 * Returns the branch on which the resulting transaction should operate.
	 *
	 * @return The branch. Never <code>null</code>.
	 */
	public String getBranch();

	/**
	 * Checks if the resulting transaction should operate on the "now" timestamp of the target branch.
	 *
	 * @return <code>true</code> if the resulting transaction should operate on the target branch, otherwise
	 *         <code>false</code>.
	 */
	public default boolean isTimestampNow() {
		return this.getTimestamp() == null;
	}

	/**
	 * Returns the timestamp on which the resulting transaction should operate.
	 *
	 * @return The configured timestamp, or <code>null</code> if the transaction should operate on the "now" timestamp
	 *         of the target branch.
	 */
	public Long getTimestamp();

}
