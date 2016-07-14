package org.chronos.chronodb.internal.impl.dump.meta;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBConstants;

public class BranchDumpMetadata {

	public static BranchDumpMetadata createMasterBranchMetadata() {
		BranchDumpMetadata metadata = new BranchDumpMetadata();
		metadata.setName(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
		metadata.setBranchingTimestamp(0L);
		metadata.setParentName(null);
		return metadata;
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

	public BranchDumpMetadata() {
		// serialization constructor
	}

	public BranchDumpMetadata(final Branch branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		this.name = branch.getName();
		if (branch.getOrigin() != null) {
			this.parentName = branch.getOrigin().getName();
		} else {
			this.parentName = null;
		}
		this.branchingTimestamp = branch.getBranchingTimestamp();
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	public String getName() {
		return this.name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String getParentName() {
		return this.parentName;
	}

	public void setParentName(final String parentName) {
		this.parentName = parentName;
	}

	public long getBranchingTimestamp() {
		return this.branchingTimestamp;
	}

	public void setBranchingTimestamp(final long branchingTimestamp) {
		this.branchingTimestamp = branchingTimestamp;
	}

}
