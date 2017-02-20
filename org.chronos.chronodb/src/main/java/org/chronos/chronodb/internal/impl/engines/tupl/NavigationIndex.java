package org.chronos.chronodb.internal.impl.engines.tupl;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException;
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.serialization.KryoManager;
import org.cojen.tupl.Cursor;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class NavigationIndex {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	public static String NAME = "navigation";

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected NavigationIndex() {
		throw new IllegalStateException("NavigationIndex.class must not be instantiated!");
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

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
	public static void insert(final TuplTransaction tx, final String branchName, final String keyspaceName,
			final String matrixName, final long timestamp) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspaceName, "Precondition violation - argument 'keyspaceName' must not be NULL!");
		checkNotNull(matrixName, "Precondition violation - argument 'matrixName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		byte[] keyspaceToMatrixBinary = tx.load(NAME, branchName);
		Map<String, KeyspaceMetadata> keyspaceToMetadata;
		if (keyspaceToMatrixBinary == null) {
			// create a new map
			keyspaceToMetadata = Maps.newHashMap();
		} else {
			keyspaceToMetadata = KryoManager.deserialize(keyspaceToMatrixBinary);
		}
		KeyspaceMetadata metadata = new KeyspaceMetadata(keyspaceName, matrixName, timestamp);
		keyspaceToMetadata.put(keyspaceName, metadata);
		tx.store(NAME, branchName, KryoManager.serialize(keyspaceToMetadata));
		logTrace("Inserting into NavigationMap. Branch = '" + branchName + "', keypsace = '" + keyspaceName
				+ "', matrix = '" + matrixName + "'");
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
	public static boolean existsBranch(final TuplTransaction tx, final String branchName) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		logTrace("Checking branch existence for branch '" + branchName + "'");
		byte[] keyspaceToMetadata = tx.load(NAME, branchName);
		return keyspaceToMetadata != null && keyspaceToMetadata.length > 0;
	}

	/**
	 * Returns the names of all branches in the Navigation Map.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 *
	 * @return An immutable set of branch names. May be empty, but never <code>null</code>.
	 */
	public static Set<String> branchNames(final TuplTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		logTrace("Retrieving branch names");
		Set<String> resultSet = Sets.newHashSet();
		Cursor cursor = tx.newCursorOn(NAME);
		try {
			// we disable auto-loading of values here because we are interested only in the keys
			cursor.autoload(false);
			cursor.first();
			while (cursor.key() != null) {
				byte[] key = cursor.key();
				resultSet.add(TuplUtils.decodeString(key));
				cursor.next();
			}
			return resultSet;
		} catch (IOException ioe) {
			throw new ChronosIOException("Failed to read branch metadata! See root cause for details.", ioe);
		} finally {
			if (cursor != null) {
				cursor.reset();
			}
		}
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
	public static void deleteBranch(final TuplTransaction tx, final String branchName)
			throws ChronoDBBranchingException {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		assertBranchExists(tx, branchName);
		logTrace("Deleting branch '" + branchName + "' in navigation map");
		tx.delete(NAME, branchName);
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
	public static Set<KeyspaceMetadata> getKeyspaceMetadata(final TuplTransaction tx, final String branchName)
			throws ChronoDBBranchingException {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		assertBranchExists(tx, branchName);
		byte[] keyspaceToMetadata = tx.load(NAME, branchName);
		if (keyspaceToMetadata == null) {
			return Sets.newHashSet();
		}
		Map<String, KeyspaceMetadata> map = KryoManager.deserialize(keyspaceToMetadata);
		return Sets.newHashSet(map.values());
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

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
	static void assertBranchExists(final TuplTransaction tx, final String branchName)
			throws ChronoDBBranchingException {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		if (existsBranch(tx, branchName) == false) {
			throw new ChronoDBBranchingException("There is no branch named '" + branchName + "'!");
		}
	}
}
