package org.chronos.chronodb.internal.impl.engines.tupl;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

public class TimeIndex {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final String NAME = "TimeMap";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected TimeIndex() {
		throw new IllegalStateException("TimeMap must not be instantiated!");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Inserts or updates the given branch and timestamp combination in the Time Map.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 * @param branchName
	 *            The name of the branch to change the "now" timestamp for. Must not be <code>null</code>. Must refer to
	 *            an existing branch.
	 * @param timestampNow
	 *            The new "now" timestamp to assign to the branch. Must not be negative.
	 */
	public static void put(final DefaultTuplTransaction tx, final String branchName, final long timestampNow) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestampNow >= 0,
				"Precondition violation - argument 'timestampNow' must be >= 0 (value: " + timestampNow + ")!");
		NavigationIndex.assertBranchExists(tx, branchName);
		logTrace("Updating TimeMap entry. Branch = '" + branchName + "', timestamp = '" + timestampNow + "'");
		tx.store(NAME, branchName, TuplUtils.encodeLong(timestampNow));
	}

	/**
	 * Removes the branch-timestamp pair from the Time Map where the branch name is equal to the given name.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 * @param branchName
	 *            The name of the branch to remove the "now" timestamp for. Must not be <code>null</code>. May refer to
	 *            a non-existing branch.
	 */
	public static void remove(final DefaultTuplTransaction tx, final String branchName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		logTrace("Removing TimeMap entry. Branch = '" + branchName + "'");
		tx.delete(NAME, branchName);
	}

	/**
	 * Returns the "now" timestamp stored in the database for the given branch.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 * @param branchName
	 *            The name of the branch to get the "now" timestamp for. Must not be <code>null</code>. Must refer to an
	 *            existing branch.
	 *
	 * @return The "now" timestamp stored in the database, or 0 if no timestamp is stored for the given branch.
	 */
	public static long get(final DefaultTuplTransaction tx, final String branchName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		NavigationIndex.assertBranchExists(tx, branchName);
		logTrace("Retrieving 'now' timestamp from TimeMap. Branch = '" + branchName + "'");
		byte[] timestampBytes = tx.load(NAME, branchName);
		if (timestampBytes == null || timestampBytes.length <= 0) {
			return 0;
		}
		Long timestamp = TuplUtils.decodeLong(timestampBytes);
		if (timestamp == null || timestamp < 0) {
			return 0;
		} else {
			return timestamp;
		}
	}

}
