package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;

/**
 * The mutable portion of a {@link org.chronos.chronodb.api.ChronoDBTransaction.Configuration}.
 * <p>
 *
 * This class is not part of the public API. Down-casting a configuration to this class means leaving the public API.
 *
 * <p>
 * This class contains methods that are intended for building up the configuration. Once the construction is complete,
 * {@link #freeze()} should be called to make the configuration unmodifiable.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface MutableTransactionConfiguration extends TransactionConfigurationInternal {

	/**
	 * Sets the name of the branch on which the resulting transaction should operate.
	 *
	 * <p>
	 * By default, this is set to {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER}.
	 *
	 * @param branchName
	 *            The branch name. Must not be <code>null</code>.
	 */
	public void setBranch(String branchName);

	/**
	 * Sets the timestamp on which the resulting transaction should operate.
	 *
	 * <p>
	 * By default, this is set to the "now" timestamp of the branch. To reset it to the now timestamp after calling this
	 * method, please use {@link #setTimestampToNow()}.
	 *
	 * @param timestamp
	 *            The timestamp. Must not be negative.
	 */
	public void setTimestamp(long timestamp);

	/**
	 * Sets the timestamp on which the resulting transaction should operate to the "now" timestamp of the target branch.
	 */
	public void setTimestampToNow();

	/**
	 * Enables or disables thread-safety on this transaction.
	 *
	 * <p>
	 * Important note: {@link ChronoDB} instances themselves are always thread-safe. This property only determines if
	 * the resulting {@link ChronoDBTransaction} itself may be shared among multiple threads or not.
	 *
	 * @param threadSafe
	 *            Set this to <code>true</code> if thread-safety on the transaction itself is required, otherwise set it
	 *            to <code>false</code> to disable thread-safety.
	 */
	public void setThreadSafe(boolean threadSafe);

	/**
	 * Sets the conflict resolution strategy to apply in case of commit conflicts.
	 *
	 * @param strategy
	 *            The strategy to use. Must not be <code>null</code>.
	 */
	public void setConflictResolutionStrategy(ConflictResolutionStrategy strategy);

	/**
	 * Sets the {@link DuplicateVersionEliminationMode} on this transaction.
	 *
	 * @param mode
	 *            The mode to use. Must not be <code>null</code>.
	 */
	public void setDuplicateVersionEliminationMode(DuplicateVersionEliminationMode mode);

	/**
	 * Sets the read-only mode of this transaction configuration.
	 *
	 * @param readOnly
	 *            Set this to <code>true</code> if the transaction should be read-only, or to <code>false</code> if it
	 *            should be read-write.
	 */
	public void setReadOnly(boolean readOnly);

	/**
	 * "Freezes" the configuration.
	 * <p>
	 *
	 * After calling this method, no further modifications to the transaction configuration will be allowed.
	 */
	public void freeze();

}
