package org.chronos.chronodb.internal.impl;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDBConstants;

/**
 * A common interface for branch metadata objects.
 *
 * <p>
 * Instances of this class should always be treated as immutable value objects.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface IBranchMetadata {

	/**
	 * Creates the {@linkplain IBranchMetadata branch metadata} object for the {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
	 *
	 * @return The branch metadata object for the master branch. Never <code>null</code>.
	 */
	public static IBranchMetadata createMasterBranchMetadata() {
		return BranchMetadata2.createMasterBranchMetadata();
	}

	/**
	 * Creates a new {@link IBranchMetadata} object.
	 *
	 * @param name
	 *            The name of the branch. Must not be <code>null</code>.
	 * @param parentName
	 *            The name of the parent branch. Must not be <code>null</code>, except if the <code>name</code> parameter is {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master}.
	 * @param branchingTimestamp
	 *            The branching timestamp. Must not be negative.
	 * @param directoryName
	 *            The name of the directory where the branch is stored. May be <code>null</code> if the current Chronos backend does not support/use directory names for branches.
	 * @return The branch metadata object. Never <code>null</code>.
	 */
	public static IBranchMetadata create(final String name, final String parentName, final long branchingTimestamp, final String directoryName) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		// parent name may be null if we have the master branch at hand
		if (ChronoDBConstants.MASTER_BRANCH_IDENTIFIER.equals(name) == false) {
			checkNotNull(parentName, "Precondition violation - argument 'parentName' must not be NULL!");
		}
		checkArgument(branchingTimestamp >= 0,
				"Precondition violation - argument 'branchingTimestamp' must not be negative!");
		return new BranchMetadata2(name, parentName, branchingTimestamp, directoryName);
	}

	/**
	 * Returns the name of the branch.
	 *
	 * @return The branch name. Never <code>null</code>.
	 */
	public String getName();

	/**
	 * Returns the name of the parent branch.
	 *
	 * @return The parent branch name. May be <code>null</code> if the current branch is the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
	 */
	public String getParentName();

	/**
	 * Returns the branching timestamp, i.e. the point in time when this branch was branched away from the parent.
	 *
	 * @return The branching timestamp. Will always be greater than or equal to zero.
	 */
	public long getBranchingTimestamp();

	/**
	 * Returns the name of the OS directory where this branch is stored.
	 *
	 * <p>
	 * This property may not be available on all backends. Backends that do not support this property should return <code>null</code>.
	 *
	 * @return The name of the directory where the branch data is located. May be <code>null</code> if unsupported.
	 *
	 * @since 0.6.0
	 */
	public String getDirectoryName();

}
