package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.impl.BranchMetadata;

import com.google.common.collect.Sets;

public abstract class AbstractBranchManager implements BranchManager, BranchManagerInternal {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	protected final ChronoDBInternal owningDb;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected AbstractBranchManager(final ChronoDBInternal owningDb) {
		checkNotNull(owningDb, "Precondition violation - argument 'owningDb' must not be NULL!");
		this.owningDb = owningDb;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Branch createBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.assertBranchNameExists(branchName, false);
		long now = this.getMasterBranch().getTemporalKeyValueStore().getNow();
		return this.createBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, branchName, now);
	}

	@Override
	public Branch createBranch(final String branchName, final long branchingTimestamp) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.assertBranchNameExists(branchName, false);
		checkArgument(branchingTimestamp >= 0,
				"Precondition violation - argument 'branchingTimestamp' must not be negative!");
		long now = this.getMasterBranch().getTemporalKeyValueStore().getNow();
		checkArgument(branchingTimestamp <= now,
				"Precondition violation - argument 'branchingTimestamp' must be less than the timestamp of the latest commit on the parent branch!");
		return this.createBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, branchName, branchingTimestamp);
	}

	@Override
	public Branch createBranch(final String parentName, final String newBranchName) {
		checkNotNull(parentName, "Precondition violation - argument 'parentName' must not be NULL!");
		checkNotNull(newBranchName, "Precondition violation - argument 'newBranchName' must not be NULL!");
		this.assertBranchNameExists(newBranchName, false);
		this.assertBranchNameExists(parentName, true);
		long now = this.getBranchInternal(parentName).getTemporalKeyValueStore().getNow();
		return this.createBranch(parentName, newBranchName, now);
	}

	@Override
	public Branch createBranch(final String parentName, final String newBranchName, final long branchingTimestamp) {
		checkNotNull(parentName, "Precondition violation - argument 'parentName' must not be NULL!");
		checkNotNull(newBranchName, "Precondition violation - argument 'newBranchName' must not be NULL!");
		this.assertBranchNameExists(parentName, true);
		this.assertBranchNameExists(newBranchName, false);
		checkArgument(branchingTimestamp >= 0,
				"Precondition violation - argument 'branchingTimestamp' must not be negative!");
		long now = this.getBranchInternal(parentName).getTemporalKeyValueStore().getNow();
		checkArgument(branchingTimestamp <= now,
				"Precondition violation - argument 'branchingTimestamp' must be less than the timestamp of the latest commit on the parent branch!");
		BranchMetadata metadata = new BranchMetadata(newBranchName, parentName, branchingTimestamp);
		return this.createBranch(metadata);
	}

	@Override
	public boolean existsBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		return this.getBranchInternal(branchName) != null;
	}

	@Override
	public Branch getBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.assertBranchNameExists(branchName, true);
		return this.getBranchInternal(branchName);
	}

	@Override
	public Set<Branch> getBranches() {
		Set<Branch> resultSet = Sets.newHashSet();
		for (String branchName : this.getBranchNames()) {
			Branch branch = this.getBranch(branchName);
			resultSet.add(branch);
		}
		return Collections.unmodifiableSet(resultSet);
	}

	// =====================================================================================================================
	// INTERNAL API
	// =====================================================================================================================

	@Override
	public void loadBranchDataFromDump(final List<BranchMetadata> branches) {
		checkNotNull(branches, "Precondition violation - argument 'branches' must not be NULL!");
		for (BranchMetadata branchMetadata : branches) {
			// assert that the branch does not yet exist
			if (this.existsBranch(branchMetadata.getName())) {
				throw new IllegalStateException(
						"There already exists a branch named '" + branchMetadata.getName() + "'!");
			}
			// assert that the parent branch does exist
			if (this.existsBranch(branchMetadata.getParentName()) == false) {
				throw new IllegalStateException(
						"Attempted to create branch '" + branchMetadata.getName() + "' on parent '"
								+ branchMetadata.getParentName() + "', but the parent branch does not exist!");
			}
			this.createBranch(branchMetadata);
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	@Override
	public BranchInternal getMasterBranch() {
		// this override is convenient because it returns the BranchInternal representation of the branch.
		return this.getBranchInternal(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	protected void assertBranchNameExists(final String branchName, final boolean exists) {
		if (exists) {
			if (this.existsBranch(branchName) == false) {
				throw new ChronoDBBranchingException("There is no branch named '" + branchName + "'!");
			}
		} else {
			if (this.existsBranch(branchName)) {
				throw new ChronoDBBranchingException("A branch with name '" + branchName + "' already exists!");
			}
		}
	}

	// =====================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =====================================================================================================================

	protected abstract BranchInternal createBranch(BranchMetadata metadata);

	protected abstract BranchInternal getBranchInternal(final String name);
}
