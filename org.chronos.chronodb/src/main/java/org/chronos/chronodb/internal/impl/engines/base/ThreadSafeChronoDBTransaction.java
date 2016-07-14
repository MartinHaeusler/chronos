package org.chronos.chronodb.internal.impl.engines.base;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.chronos.chronodb.api.ChangeSetEntry;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;

public class ThreadSafeChronoDBTransaction extends StandardChronoDBTransaction {

	private final Lock changeSetLock = new ReentrantLock(true);

	public ThreadSafeChronoDBTransaction(final TemporalKeyValueStore tkvs, final long timestamp,
			final String branchIdentifier, final Configuration configuration) {
		super(tkvs, timestamp, branchIdentifier, configuration);
	}

	@Override
	protected void putInternal(final QualifiedKey key, final Object value, final PutOption[] options) {
		this.changeSetLock.lock();
		try {
			ChangeSetEntry entry = ChangeSetEntry.createChange(key, value, options);
			this.changeSet.add(entry);
		} finally {
			this.changeSetLock.unlock();
		}
	}

	@Override
	protected void removeInternal(final QualifiedKey key) {
		this.changeSetLock.lock();
		try {
			ChangeSetEntry entry = ChangeSetEntry.createDeletion(key);
			this.changeSet.add(entry);
		} finally {
			this.changeSetLock.unlock();
		}
	}

}
