package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import org.chronos.chronodb.api.MaintenanceManager;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.common.autolock.AutoLock;

public class ChunkDbMaintenanceManager implements MaintenanceManager {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final ChunkedChronoDB owningDB;

	private final Lock rolloverLock = new ReentrantLock(true);

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChunkDbMaintenanceManager(final ChunkedChronoDB owningDB) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		this.owningDB = owningDB;
	}

	// =================================================================================================================
	// PUBLIC API [ BRANCHING ]
	// =================================================================================================================

	@Override
	public boolean isRolloverSupported() {
		return true;
	}

	@Override
	public void performRolloverOnBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.rolloverLock.lock();
		try {
			// assert that the branch exists
			BranchInternal branch = (BranchInternal) this.owningDB.getBranchManager().getBranch(branchName);
			if (branch == null) {
				throw new IllegalArgumentException("There is no branch named '" + branchName + "', cannot perform rollover!");
			}
			ChunkDbTkvs tkvs = (ChunkDbTkvs) branch.getTemporalKeyValueStore();
			tkvs.performRollover();
		} finally {
			this.rolloverLock.unlock();
		}
	}

	@Override
	public void performRolloverOnAllBranches() {
		this.rolloverLock.lock();
		try {
			try (AutoLock lock = this.owningDB.lockExclusive()) {
				// note: JavaDoc states explicitly that this method does not require ACID safety,
				// so it's ok to roll over the branches one by one.
				for (String branchName : this.owningDB.getBranchManager().getBranchNames()) {
					this.performRolloverOnBranch(branchName);
				}
			}
		} finally {
			this.rolloverLock.unlock();
		}
	}

	@Override
	public void performRolloverOnAllBranchesWhere(final Predicate<String> branchPredicate) {
		checkNotNull(branchPredicate, "Precondition violation - argument 'branchPredicate' must not be NULL!");
		this.rolloverLock.lock();
		try {
			try (AutoLock lock = this.owningDB.lockExclusive()) {
				// note: JavaDoc states explicitly that this method does not require ACID safety,
				// so it's ok to roll over the branches one by one.
				for (String branchName : this.owningDB.getBranchManager().getBranchNames()) {
					if (branchPredicate.test(branchName) == false) {
						// predicate says no...
						continue;
					}
					this.performRolloverOnBranch(branchName);
				}
			}
		} finally {
			this.rolloverLock.unlock();
		}
	}

}
