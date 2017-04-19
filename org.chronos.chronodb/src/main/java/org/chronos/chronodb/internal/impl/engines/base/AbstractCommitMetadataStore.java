package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.internal.api.CommitMetadataStore;

public abstract class AbstractCommitMetadataStore implements CommitMetadataStore {

	private final ChronoDB owningDB;
	private final Branch owningBranch;
	private final ReadWriteLock lock;

	protected AbstractCommitMetadataStore(final ChronoDB owningDB, final Branch owningBranch) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		checkNotNull(owningBranch, "Precondition violation - argument 'owningBranch' must not be NULL!");
		this.owningDB = owningDB;
		this.owningBranch = owningBranch;
		this.lock = new ReentrantReadWriteLock(true);
	}

	@Override
	public void put(final long commitTimestamp, final Object commitMetadata) {
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		// serialize the metadata object
		byte[] serializedMetadata = this.serialize(commitMetadata);
		this.lock.writeLock().lock();
		try {
			this.putInternal(commitTimestamp, serializedMetadata);
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public Object get(final long commitTimestamp) {
		checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		byte[] serializedValue = null;
		this.lock.readLock().lock();
		try {
			// retrieve the byte array result
			serializedValue = this.getInternal(commitTimestamp);
		} finally {
			this.lock.readLock().unlock();
		}
		if (serializedValue == null || serializedValue.length <= 0) {
			// in this case we couldn't find a commit, or the assigned metadata was null.
			return null;
		} else {
			// we found a commit with metadata; deserialize the value and return it
			return this.getSerializationManager().deserialize(serializedValue);
		}
	}

	@Override
	public List<Long> getCommitTimestampsAround(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		return this.getCommitMetadataAround(timestamp, count).stream().map(entry -> entry.getKey()).collect(Collectors.toList());
	}

	@Override
	public List<Long> getCommitTimestampsBefore(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		return this.getCommitMetadataBefore(timestamp, count).stream().map(entry -> entry.getKey()).collect(Collectors.toList());
	}

	@Override
	public List<Long> getCommitTimestampsAfter(final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		return this.getCommitMetadataAfter(timestamp, count).stream().map(entry -> entry.getKey()).collect(Collectors.toList());
	}

	@Override
	public void rollbackToTimestamp(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		this.lock.writeLock().lock();
		try {
			this.rollbackToTimestampInternal(timestamp);
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public Branch getOwningBranch() {
		return this.owningBranch;
	}

	// =====================================================================================================================
	// UTILITY METHODS
	// =====================================================================================================================

	protected SerializationManager getSerializationManager() {
		return this.owningDB.getSerializationManager();
	}

	protected ChronoDB getOwningDB() {
		return this.owningDB;
	}

	protected String getBranchName() {
		return this.getOwningBranch().getName();
	}

	protected <A, B> Pair<A, B> mapEntryToPair(final Entry<A, B> entry) {
		return Pair.of(entry.getKey(), entry.getValue());
	}

	@SuppressWarnings("unchecked")
	protected <A, B> Pair<A, B> mapSerialEntryToPair(final Entry<A, byte[]> entry) {
		return (Pair<A, B>) Pair.of(entry.getKey(), this.deserialize(entry.getValue()));
	}

	protected byte[] serialize(final Object value) {
		if (value == null) {
			// serialize NULL values as byte arrays of length 0
			return new byte[0];
		}
		return this.getSerializationManager().serialize(value);
	}

	protected Object deserialize(final byte[] serialForm) {
		if (serialForm == null || serialForm.length <= 0) {
			return null;
		}
		return this.getSerializationManager().deserialize(serialForm);
	}

	protected Entry<Long, Object> deserializeValueOf(final Entry<Long, byte[]> entry) {
		if (entry.getValue() == null) {
			return Pair.of(entry.getKey(), null);
		} else {
			return Pair.of(entry.getKey(), this.deserialize(entry.getValue()));
		}
	}

	// =====================================================================================================================
	// ABSTRAC METHOD DECLARATIONS
	// =====================================================================================================================

	protected abstract byte[] getInternal(long timestamp);

	protected abstract void putInternal(long commitTimestamp, byte[] metadata);

	protected abstract void rollbackToTimestampInternal(long timestamp);

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	protected static class EntryTimestampComparator implements Comparator<Entry<Long, ?>> {

		public static final EntryTimestampComparator INSTANCE = new EntryTimestampComparator();

		@Override
		public int compare(final Entry<Long, ?> o1, final Entry<Long, ?> o2) {
			if (o1 == null && o2 == null) {
				return 0;
			} else if (o1 != null && o2 == null) {
				return 1;
			} else if (o1 == null && o2 != null) {
				return -1;
			}
			return o1.getKey().compareTo(o2.getKey());
		}

	}
}
