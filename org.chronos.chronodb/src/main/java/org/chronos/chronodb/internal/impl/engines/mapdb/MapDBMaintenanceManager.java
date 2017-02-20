package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;

import java.util.function.Predicate;

import org.chronos.chronodb.api.MaintenanceManager;

public class MapDBMaintenanceManager implements MaintenanceManager {

	@SuppressWarnings("unused")
	private final MapDBChronoDB owningDB;

	public MapDBMaintenanceManager(final MapDBChronoDB owningDB) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		this.owningDB = owningDB;
	}

	// =================================================================================================================
	// ROLLOVER
	// =================================================================================================================

	@Override
	public boolean isRolloverSupported() {
		return false;
	}

	@Override
	public void performRolloverOnBranch(final String branchName) {
		throw new UnsupportedOperationException("The MapDB backend does not support rollover operations.");
	}

	@Override
	public void performRolloverOnAllBranches() {
		throw new UnsupportedOperationException("The MapDB backend does not support rollover operations.");
	}

	@Override
	public void performRolloverOnAllBranchesWhere(final Predicate<String> branchPredicate) {
		throw new UnsupportedOperationException("The MapDB backend does not support rollover operations.");
	}

}
