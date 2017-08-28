package org.chronos.chronodb.internal.impl;

import static com.google.common.base.Preconditions.*;

import java.io.Serializable;

import org.chronos.chronodb.api.ChronoDBConstants;

/**
 * An enhanced version of {@link BranchMetadata} that additionally includes the {@link IBranchMetadata#getDirectoryName() directory name} property.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @since 0.6.0
 */
public class BranchMetadata2 implements Serializable, IBranchMetadata {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	public static BranchMetadata2 createMasterBranchMetadata() {
		return new BranchMetadata2(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, null, 0L, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String name;
	private String parentName;
	private String directoryName;
	private long branchingTimestamp;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected BranchMetadata2() {
		// default constructor for serialization
	}

	public BranchMetadata2(final String name, final String parentName, final long branchingTimestamp, final String directoryName) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		// parent name may be null if we have the master branch at hand
		if (ChronoDBConstants.MASTER_BRANCH_IDENTIFIER.equals(name) == false) {
			checkNotNull(parentName, "Precondition violation - argument 'parentName' must not be NULL!");
		}
		checkArgument(branchingTimestamp >= 0,
				"Precondition violation - argument 'branchingTimestamp' must not be negative!");
		this.name = name;
		this.parentName = parentName;
		this.branchingTimestamp = branchingTimestamp;
		this.directoryName = directoryName;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public String getParentName() {
		return this.parentName;
	}

	@Override
	public long getBranchingTimestamp() {
		return this.branchingTimestamp;
	}

	@Override
	public String getDirectoryName() {
		return this.directoryName;
	}

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (this.branchingTimestamp ^ this.branchingTimestamp >>> 32);
		result = prime * result + (this.directoryName == null ? 0 : this.directoryName.hashCode());
		result = prime * result + (this.name == null ? 0 : this.name.hashCode());
		result = prime * result + (this.parentName == null ? 0 : this.parentName.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		BranchMetadata2 other = (BranchMetadata2) obj;
		if (this.branchingTimestamp != other.branchingTimestamp) {
			return false;
		}
		if (this.directoryName == null) {
			if (other.directoryName != null) {
				return false;
			}
		} else if (!this.directoryName.equals(other.directoryName)) {
			return false;
		}
		if (this.name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!this.name.equals(other.name)) {
			return false;
		}
		if (this.parentName == null) {
			if (other.parentName != null) {
				return false;
			}
		} else if (!this.parentName.equals(other.parentName)) {
			return false;
		}
		return true;
	}

}
