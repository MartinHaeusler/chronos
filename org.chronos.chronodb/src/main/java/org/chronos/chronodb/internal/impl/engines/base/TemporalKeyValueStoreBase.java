package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.builder.transaction.ChronoDBTransactionBuilder;
import org.chronos.chronodb.api.exceptions.ChronoDBTransactionException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;
import org.chronos.chronodb.internal.api.MutableTransactionConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.TransactionConfigurationInternal;
import org.chronos.chronodb.internal.impl.DefaultTransactionConfiguration;
import org.chronos.chronodb.internal.util.ThreadBound;
import org.chronos.common.autolock.AbstractAutoLock;
import org.chronos.common.autolock.AutoLock;

public abstract class TemporalKeyValueStoreBase implements TemporalKeyValueStore {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	/**
	 * The branch lock protects a single branch from illegal concurrent access.
	 * <p>
	 * The vast majority of requests will only require a read lock, including (but not limited to):
	 * <ul>
	 * <li>Read operations
	 * <li>Commits
	 * </ul>
	 *
	 * An example for a process which does indeed require the write lock (i.e. exclusive lock) is a re-indexing process,
	 * as index values will be invalid during this process, which makes reads pointless.
	 *
	 * <p>
	 * Please note that acquiring any lock (read or write) on a branch is legal if and only if the currrent thread holds
	 * a read lock on the owning database as well.
	 */
	private final ReadWriteLock branchLock = new ReentrantReadWriteLock(true);

	private final ThreadBound<AutoLock> nonExclusiveLockHolder = ThreadBound.createWeakReference();
	private final ThreadBound<AutoLock> exclusiveLockHolder = ThreadBound.createWeakReference();
	private final ThreadBound<AutoLock> branchExclusiveLockHolder = ThreadBound.createWeakReference();

	// =================================================================================================================
	// BRANCH LOCKING
	// =================================================================================================================

	@Override
	public AutoLock lockNonExclusive() {
		AutoLock lockHolder = this.nonExclusiveLockHolder.get();
		if (lockHolder == null) {
			lockHolder = new NonExclusiveAutoLock();
			this.nonExclusiveLockHolder.set(lockHolder);
		}
		lockHolder.acquireLock();
		return lockHolder;
	}

	@Override
	public AutoLock lockBranchExclusive() {
		AutoLock lockHolder = this.branchExclusiveLockHolder.get();
		if (lockHolder == null) {
			lockHolder = new BranchExclusiveAutoLock();
			this.branchExclusiveLockHolder.set(lockHolder);
		}
		lockHolder.acquireLock();
		return lockHolder;
	}

	@Override
	public AutoLock lockExclusive() {
		AutoLock lockHolder = this.exclusiveLockHolder.get();
		if (lockHolder == null) {
			lockHolder = new ExclusiveAutoLock();
			this.exclusiveLockHolder.set(lockHolder);
		}
		lockHolder.acquireLock();
		return lockHolder;
	}

	// =================================================================================================================
	// OPERATION [ TX ]
	// =================================================================================================================

	@Override
	public ChronoDBTransactionBuilder txBuilder() {
		return this.getOwningDB().txBuilder().onBranch(this.getOwningBranch());
	}

	@Override
	public ChronoDBTransaction tx(final TransactionConfigurationInternal configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		try (AutoLock lock = this.lockNonExclusive()) {
			String branchName = configuration.getBranch();
			long timestamp;
			if (configuration.isTimestampNow()) {
				timestamp = this.getNow();
			} else {
				timestamp = configuration.getTimestamp();
			}
			if (timestamp > this.getNow()) {
				throw new InvalidTransactionTimestampException(
						"Cannot open transaction at the given date or timestamp: it's after the latest commit!");
			}
			if (branchName.equals(this.getOwningBranch().getName()) == false) {
				throw new InvalidTransactionBranchException("Cannot start transaction on branch '"
						+ this.getOwningBranch().getName() + "' when transaction configuration specifies branch '"
						+ configuration.getBranch() + "'!");
			}
			if (configuration.isThreadSafe()) {
				return new ThreadSafeChronoDBTransaction(this, timestamp, branchName, configuration);
			} else {
				return new StandardChronoDBTransaction(this, timestamp, branchName, configuration);
			}
		}
	}

	@Override
	public ChronoDBTransaction txInternal(final String branch, final long timestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		MutableTransactionConfiguration configuration = new DefaultTransactionConfiguration();
		configuration.setTimestamp(timestamp);
		configuration.setBranch(branch);
		return new StandardChronoDBTransaction(this, timestamp, branch, configuration);
	}

	// =================================================================================================================
	// ABSTRACT METHODS
	// =================================================================================================================

	/**
	 * Verification method for implementations of this class.
	 *
	 * <p>
	 * The implementing method can perform arbitrary consistency checks on the given, newly created transaction before
	 * it is passed back to application code for processing.
	 *
	 * <p>
	 * If the implementation of this method detects any conflicts, it should throw an appropriate subclass of
	 * {@link ChronoDBTransactionException}.
	 *
	 * @param tx
	 *            The transaction to verify. Must not be <code>null</code>.
	 */
	protected abstract void verifyTransaction(ChronoDBTransaction tx);

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private class ExclusiveAutoLock extends AbstractAutoLock {

		private final AutoLock dbLockHolder;

		private ExclusiveAutoLock() {
			super();
			// acquire the db lock holder...
			this.dbLockHolder = TemporalKeyValueStoreBase.this.getOwningDB().lockExclusive();
			// ... but don't keep the lock for now (until 'doLock' is called)
			this.dbLockHolder.releaseLock();
		}

		@Override
		protected void doLock() {
			this.dbLockHolder.acquireLock();
			TemporalKeyValueStoreBase.this.branchLock.writeLock().lock();
		}

		@Override
		protected void doUnlock() {
			TemporalKeyValueStoreBase.this.branchLock.writeLock().unlock();
			this.dbLockHolder.releaseLock();
		}

	}

	private class BranchExclusiveAutoLock extends AbstractAutoLock {

		private final AutoLock dbLockHolder;

		private BranchExclusiveAutoLock() {
			super();
			// TODO c-39: We need to acquire the exclusive DB lock here to avoid deadlocks...
			// acquire the db lock holder...
			this.dbLockHolder = TemporalKeyValueStoreBase.this.getOwningDB().lockExclusive();
			// ... but don't keep the lock for now (until 'doLock' is called)
			this.dbLockHolder.releaseLock();
		}

		@Override
		protected void doLock() {
			this.dbLockHolder.acquireLock();
			TemporalKeyValueStoreBase.this.branchLock.writeLock().lock();
		}

		@Override
		protected void doUnlock() {
			TemporalKeyValueStoreBase.this.branchLock.writeLock().unlock();
			this.dbLockHolder.releaseLock();
		}

	}

	private class NonExclusiveAutoLock extends AbstractAutoLock {

		private final AutoLock dbLockHolder;

		private NonExclusiveAutoLock() {
			super();
			// acquire the db lock holder...
			this.dbLockHolder = TemporalKeyValueStoreBase.this.getOwningDB().lockNonExclusive();
			// ... but don't keep the lock for now (until 'doLock' is called)
			this.dbLockHolder.releaseLock();
		}

		@Override
		protected void doLock() {
			this.dbLockHolder.acquireLock();
			TemporalKeyValueStoreBase.this.branchLock.readLock().lock();
		}

		@Override
		protected void doUnlock() {
			TemporalKeyValueStoreBase.this.branchLock.readLock().unlock();
			this.dbLockHolder.releaseLock();
		}

	}

}
