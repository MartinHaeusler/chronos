package org.chronos.chronodb.internal.impl.mapdb;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.internal.impl.IBranchMetadata;

import com.google.common.collect.Sets;

public class BranchMetadataMap {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	public static final String NAME = "BranchMetadata";

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public static void insertOrUpdate(final MapDBTransaction tx, final IBranchMetadata metadata) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		Map<String, IBranchMetadata> map = getMapForWriting(tx);
		map.put(metadata.getName(), metadata);
	}

	public static IBranchMetadata getMetadata(final MapDBTransaction tx, final String name) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		Map<String, IBranchMetadata> map = getMapForReading(tx);
		return map.get(name);
	}

	public static Set<IBranchMetadata> values(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return Sets.newHashSet(getMapForReading(tx).values());
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	/**
	 * Returns the Navigation Map for the given transaction, for read-write access.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 *
	 * @return The Navigation Map for read-write access. May be empty, but never <code>null</code>.
	 */
	private static Map<String, IBranchMetadata> getMapForWriting(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.treeMap(NAME);
	}

	/**
	 * Returns the Navigation Map for the given transaction, for read-only access.
	 *
	 * @param tx
	 *            The transaction to work on. Must not be <code>null</code>, must be open.
	 *
	 * @return The Navigation Map for read-only access. May be empty, but never <code>null</code>.
	 */
	private static Map<String, IBranchMetadata> getMapForReading(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		if (tx.exists(NAME)) {
			return Collections.unmodifiableMap(tx.treeMap(NAME));
		} else {
			return Collections.emptyMap();
		}
	}

}
