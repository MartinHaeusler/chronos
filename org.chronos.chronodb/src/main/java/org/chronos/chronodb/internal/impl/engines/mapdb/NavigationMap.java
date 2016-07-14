package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;
import static org.chronos.common.logging.ChronoLogger.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException;
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

class NavigationMap {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final String NAME = "NavigationMap";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected NavigationMap() {
		throw new IllegalStateException("NavigationMap.class must not be instantiated!");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Performs an insertion into the Navigation Map.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 * @param branchName
	 *            The name of the branch to insert. Must not be <code>null</code>.
	 * @param keyspaceName
	 *            The name of the keyspace to insert. Must not be <code>null</code>.
	 * @param matrixName
	 *            The name of the matrix to insert. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp at which to create the matrix. Must not be negative.
	 */
	public static void insert(final MapDBTransaction tx, final String branchName, final String keyspaceName, final String matrixName, final long timestamp) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(matrixName, "Precondition violation - argument 'matrixName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		Map<String, Map<String, KeyspaceMetadata>> navigationMap = getMap(tx);
		Map<String, KeyspaceMetadata> keyspaceToMatrix = navigationMap.get(branchName);
		if (keyspaceToMatrix == null) {
			// create a new map
			keyspaceToMatrix = Maps.newHashMap();
		} else {
			// create a map duplicate, as MapDB is vulnerable against changes in the values
			keyspaceToMatrix = Maps.newHashMap(keyspaceToMatrix);
		}
		KeyspaceMetadata metadata = new KeyspaceMetadata(keyspaceName, matrixName, timestamp);
		keyspaceToMatrix.put(keyspaceName, metadata);
		navigationMap.put(branchName, keyspaceToMatrix);
		logTrace("Inserting into NavigationMap. Branch = '" + branchName + "', keypsace = '" + keyspaceName + "', matrix = '" + matrixName + "'");
	}

	/**
	 * Checks if the branch with the given name exists in the Navigation Map.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 * @param branchName
	 *            The name of the branch to check existence for. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if there exists a branch with the given name, otherwise <code>false</code>.
	 */
	public static boolean existsBranch(final MapDBTransaction tx, final String branchName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		logTrace("Checking branch existence for branch '" + branchName + "'");
		Map<String, Map<String, KeyspaceMetadata>> navigationMap = getMap(tx);
		return navigationMap.containsKey(branchName);
	}

	/**
	 * Returns the names of all branches in the Navigation Map.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 *
	 * @return An immutable set of branch names. May be empty, but never <code>null</code>.
	 */
	public static Set<String> branchNames(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		logTrace("Retrieving branch names");
		Map<String, Map<String, KeyspaceMetadata>> navigationMap = getMap(tx);
		Set<String> keySet = Sets.newHashSet(navigationMap.keySet());
		return Collections.unmodifiableSet(keySet);
	}

	/**
	 * Deletes the branch with the given name from the Navigation Map.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 * @param branchName
	 *            The name of the branch to delete. Must not be <code>null</code>. Must refer to an existing branch.
	 *
	 * @throws ChronoDBBranchingException
	 *             Thrown if there is no branch with the given name.
	 */
	public static void deleteBranch(final MapDBTransaction tx, final String branchName) throws ChronoDBBranchingException {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		assertBranchExists(tx, branchName);
		logTrace("Deleting branch '" + branchName + "' in navigation map");
		Map<String, Map<String, KeyspaceMetadata>> navigationMap = getMap(tx);
		navigationMap.remove(branchName);
	}

	/**
	 * Returns the map from keyspace name to matrix map name for the given branch.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 * @param branchName
	 *            The name of the branch to get the keyspace-to-matrix-name map for. Must not be <code>null</code>, must
	 *            refer to an existing branch.
	 *
	 * @return The metadata for all known keyspaces in the given branch.
	 *
	 * @throws ChronoDBBranchingException
	 *             Thrown if there is no branch with the given name.
	 */
	public static Set<KeyspaceMetadata> getKeyspaceMetadata(final MapDBTransaction tx, final String branchName) throws ChronoDBBranchingException {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		assertBranchExists(tx, branchName);
		Map<String, KeyspaceMetadata> map = getMap(tx).get(branchName);
		Set<KeyspaceMetadata> resultSet = Sets.newHashSet(map.values());
		return resultSet;
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	/**
	 * Returns the Navigation Map for the given transaction.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 *
	 * @return The Navigation Map. May be empty, but never <code>null</code>.
	 */
	private static Map<String, Map<String, KeyspaceMetadata>> getMap(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.treeMap(NAME);
	}

	/**
	 * Asserts that a branch with the given name exists.
	 *
	 * <p>
	 * If there is a branch with the given name, then this method does nothing and returns immediately. Otherwise, a
	 * {@link ChronoDBBranchingException} is thrown.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 * @param branchName
	 *            The name of the branch to check. Must not be <code>null</code>.
	 *
	 * @throws ChronoDBBranchingException
	 *             Thrown if there is no branch with the given name.
	 */
	static void assertBranchExists(final MapDBTransaction tx, final String branchName) throws ChronoDBBranchingException {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		if (existsBranch(tx, branchName) == false) {
			throw new ChronoDBBranchingException("There is no branch named '" + branchName + "'!");
		}
	}

}