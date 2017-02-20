package org.chronos.chronodb.api;

import java.util.function.Predicate;

import org.chronos.chronodb.internal.impl.engines.chunkdb.ChronoChunk;

/**
 * The {@link MaintenanceManager} offers maintenance operations on the database.
 *
 * <p>
 * In general, {@link ChronoDB} is intended as a low-maintenance database, therefore the number of operations in this class is limited.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface MaintenanceManager {

	/**
	 * Checks if this database supports the <i>rollover</i> mechanism.
	 *
	 * <p>
	 * A rollover refers to the process of extracting the head version of the current database state, and copying it into a new container, called a <i>chunk</i> (see also: {@link ChronoChunk}). This limits the amount of historical data per chunk, allowing for faster queries in each individual chunk. However, as each rollover duplicates the head revision, it also increases the memory footprint of the database on disk.
	 *
	 * @return <code>true</code> if this database supports the rollover operation, otherwise <code>false</code>.
	 */
	public boolean isRolloverSupported();

	/**
	 * Performs a rollover on the branch with the given name.
	 *
	 * <p>
	 * Not all backends support this operation. Please use {@link #isRolloverSupported()} first to check if this operation is supported or not.
	 *
	 * @param branchName
	 *            The branch name to roll over. Must not be <code>null</code>, must refer to an existing branch.
	 *
	 * @throws UnsupportedOperationException
	 *             Thrown if this backend {@linkplain #isRolloverSupported() does not support rollovers}.
	 */
	public void performRolloverOnBranch(String branchName);

	/**
	 * Performs a rollover on the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
	 *
	 * <p>
	 * Not all backends support this operation. Please use {@link #isRolloverSupported()} first to check if this operation is supported or not.
	 *
	 * @throws UnsupportedOperationException
	 *             Thrown if this backend {@linkplain #isRolloverSupported() does not support rollovers}.
	 */
	public default void performRolloverOnMaster() {
		this.performRolloverOnBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	/**
	 * Performs a rollover on all existing branches.
	 *
	 * <p>
	 * Not all backends support this operation. Please use {@link #isRolloverSupported()} first to check if this operation is supported or not.
	 *
	 * <p>
	 * <b>Important note:</b> This method is <b>not guaranteed to be ACID safe</b>. Rollovers will be executed one after the other. If an unexpected event (such as JVM crash, exceptions, power supply failure...) occurs, then some branches may have been rolled over while others have not.
	 *
	 * <p>
	 * This is a <b>very</b> expensive operation that can substantially increase the memory footprint of the database on disk. Use with care.
	 *
	 * @throws UnsupportedOperationException
	 *             Thrown if this backend {@linkplain #isRolloverSupported() does not support rollovers}.
	 */
	public void performRolloverOnAllBranches();

	/**
	 * Performs a rollover on all branches that match the given predicate.
	 *
	 * <p>
	 * Not all backends support this operation. Please use {@link #isRolloverSupported()} first to check if this operation is supported or not.
	 *
	 * <p>
	 * <b>Important note:</b> This method is <b>not guaranteed to be ACID safe</b>. Rollovers will be executed one after the other. If an unexpected event (such as JVM crash, exceptions, power supply failure...) occurs, then some branches may have been rolled over while others have not.
	 *
	 * <p>
	 * This is a potentially <b>very</b> expensive operation that can substantially increase the memory footprint of the database on disk. Use with care.
	 *
	 * @param branchPredicate
	 *            The predicate that decides whether or not to roll over the branch in question. Must not be <code>null</code>.
	 *
	 * @throws UnsupportedOperationException
	 *             Thrown if this backend {@linkplain #isRolloverSupported() does not support rollovers}.
	 */
	public void performRolloverOnAllBranchesWhere(Predicate<String> branchPredicate);

}
