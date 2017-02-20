package org.chronos.chronodb.internal.impl.mapdb;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;

public class TimeMap {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final String NAME = "TimeMap";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected TimeMap() {
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
	public static void put(final MapDBTransaction tx, final String branchName, final long timestampNow) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestampNow >= 0,
				"Precondition violation - argument 'timestampNow' must be >= 0 (value: " + timestampNow + ")!");
		NavigationMap.assertBranchExists(tx, branchName);
		logTrace("Updating TimeMap entry. Branch = '" + branchName + "', timestamp = '" + timestampNow + "'");
		getMapForWriting(tx).put(branchName, timestampNow);
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
	public static void remove(final MapDBTransaction tx, final String branchName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		logTrace("Removing TimeMap entry. Branch = '" + branchName + "'");
		getMapForWriting(tx).remove(branchName);
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
	public static long get(final MapDBTransaction tx, final String branchName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		NavigationMap.assertBranchExists(tx, branchName);
		logTrace("Retrieving 'now' timestamp from TimeMap. Branch = '" + branchName + "'");
		Long timestamp = getMapForReading(tx).get(branchName);
		if (timestamp == null || timestamp < 0) {
			return 0;
		} else {
			return timestamp;
		}
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	/**
	 * Returns the branch-to-timestamp map for the given transaction in read-write mode.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 *
	 * @return The map from branch names to "now" timestamps in read-write mode. May be empty, but never
	 *         <code>null</code>.
	 */
	private static Map<String, Long> getMapForWriting(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.treeMap(NAME);
	}

	/**
	 * Returns the branch-to-timestamp map for the given transaction in read-only mode.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 *
	 * @return The map from branch names to "now" timestamps in read-only mode. May be empty, but never
	 *         <code>null</code>.
	 */
	private static Map<String, Long> getMapForReading(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		if (tx.exists(NAME)) {
			return Collections.unmodifiableMap(tx.treeMap(NAME));
		} else {
			return Collections.emptyMap();
		}
	}

}