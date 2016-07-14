package org.chronos.chronodb.internal.impl;

import static com.google.common.base.Preconditions.*;

import java.io.Serializable;

import org.chronos.chronodb.api.ChronoDBConstants;

public class BranchMetadata implements Serializable {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	public static BranchMetadata createMasterBranchMetadata() {
		return new BranchMetadata(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, null, 0L);
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String name;
	private String parentName;
	private long branchingTimestamp;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected BranchMetadata() {
		// default constructor for serialization
	}

	public BranchMetadata(final String name, final String parentName, final long branchingTimestamp) {
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
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public String getName() {
		return this.name;
	}

	public String getParentName() {
		return this.parentName;
	}

	public long getBranchingTimestamp() {
		return this.branchingTimestamp;
	}

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (this.branchingTimestamp ^ this.branchingTimestamp >>> 32);
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
		BranchMetadata other = (BranchMetadata) obj;
		if (this.branchingTimestamp != other.branchingTimestamp) {
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
