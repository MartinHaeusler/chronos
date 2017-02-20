package org.chronos.chronodb.internal.impl.engines.tupl;

import static com.google.common.base.Preconditions.*;

import java.util.function.Predicate;

import org.chronos.chronodb.api.MaintenanceManager;

public class TuplMaintenanceManager implements MaintenanceManager {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	@SuppressWarnings("unused")
	private final TuplChronoDB owningDB;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public TuplMaintenanceManager(final TuplChronoDB tuplChronoDB) {
		checkNotNull(tuplChronoDB, "Precondition violation - argument 'tuplChronoDB' must not be NULL!");
		this.owningDB = tuplChronoDB;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public boolean isRolloverSupported() {
		return false;
	}

	@Override
	public void performRolloverOnBranch(final String branchName) {
		throw new UnsupportedOperationException("Tupl backend does not support rollover!");
	}

	@Override
	public void performRolloverOnAllBranches() {
		throw new UnsupportedOperationException("Tupl backend does not support rollover!");
	}

	@Override
	public void performRolloverOnAllBranchesWhere(final Predicate<String> branchPredicate) {
		throw new UnsupportedOperationException("Tupl backend does not support rollover!");
	}

}
