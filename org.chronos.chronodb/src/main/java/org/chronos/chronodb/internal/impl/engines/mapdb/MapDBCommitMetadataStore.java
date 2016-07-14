package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;

import java.util.NavigableMap;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.internal.impl.engines.base.AbstractCommitMetadataStore;
import org.mapdb.Serializer;

public class MapDBCommitMetadataStore extends AbstractCommitMetadataStore {

	private static final String MAP_SUFFIX = "_CommitMetadata";

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected MapDBCommitMetadataStore(final MapDBChronoDB owningDB, final Branch owningBranch) {
		super(owningDB, owningBranch);
	}

	// =====================================================================================================================
	// API IMPLEMENTATION
	// =====================================================================================================================

	@Override
	protected void putInternal(final long commitTimestamp, final byte[] metadata) {
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		checkArgument(metadata.length > 0,
				"Precondition violation - argument 'metadata' must not be a zero-length array!");
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<Long, byte[]> map = this.getMap(tx);
			map.put(commitTimestamp, metadata);
			tx.commit();
		}
	}

	@Override
	protected byte[] getInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<Long, byte[]> map = this.getMap(tx);
			return map.get(timestamp);
		}
	}

	@Override
	protected void rollbackToTimestampInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (MapDBTransaction tx = this.openTransaction()) {
			NavigableMap<Long, byte[]> map = this.getMap(tx);
			NavigableMap<Long, byte[]> subMap = map.subMap(timestamp, false, Long.MAX_VALUE, true);
			subMap.clear();
			tx.commit();
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	@Override
	protected MapDBChronoDB getOwningDB() {
		return (MapDBChronoDB) super.getOwningDB();
	}

	private MapDBTransaction openTransaction() {
		return this.getOwningDB().openTransaction();
	}

	private NavigableMap<Long, byte[]> getMap(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.treeMap(this.getBranchName() + MAP_SUFFIX, Serializer.LONG, Serializer.BYTE_ARRAY);
	}

}
