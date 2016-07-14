package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.internal.impl.BranchMetadata;

import com.google.common.collect.Sets;

class BranchMetadataMap {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	public static final String NAME = "BranchMetadata";

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public static void insertOrUpdate(final MapDBTransaction tx, final BranchMetadata metadata) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		Map<String, BranchMetadata> map = getMap(tx);
		map.put(metadata.getName(), metadata);
	}

	public static BranchMetadata getMetadata(final MapDBTransaction tx, final String name) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		Map<String, BranchMetadata> map = getMap(tx);
		return map.get(name);
	}

	public static Set<BranchMetadata> values(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return Sets.newHashSet(getMap(tx).values());
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
	private static Map<String, BranchMetadata> getMap(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.treeMap(NAME);
	}

}
