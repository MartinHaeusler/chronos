package org.chronos.chronodb.internal.impl.engines.inmemory;

import static com.google.common.base.Preconditions.*;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.impl.engines.base.AbstractCommitMetadataStore;

public class InMemoryCommitMetadataStore extends AbstractCommitMetadataStore {

	private NavigableMap<Long, byte[]> commitMetadatMap;

	public InMemoryCommitMetadataStore(final ChronoDB owningDB, final Branch owningBranch) {
		super(owningDB, owningBranch);
		this.commitMetadatMap = new ConcurrentSkipListMap<>();
	}

	@Override
	protected byte[] getInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return this.commitMetadatMap.get(timestamp);
	}

	@Override
	protected void putInternal(final long commitTimestamp, final byte[] serializedMetadata) {
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		checkNotNull(serializedMetadata, "Precondition violation - argument 'serializedMetadata' must not be NULL!");
		checkArgument(serializedMetadata.length > 0,
				"Precondition violation - argument 'serializedMetadata' must not be a zero-length array!");
		this.commitMetadatMap.put(commitTimestamp, serializedMetadata);
	}

	@Override
	protected void rollbackToTimestampInternal(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		NavigableMap<Long, byte[]> subMap = this.commitMetadatMap.subMap(timestamp, false, Long.MAX_VALUE, true);
		subMap.clear();
	}

}
