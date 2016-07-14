package org.chronos.chronodb.internal.impl;

import static com.google.common.base.Preconditions.*;

import java.util.List;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;

import com.google.common.collect.Lists;

public class BranchImpl implements BranchInternal {

	// =====================================================================================================================
	// STATIC FACTORY
	// =====================================================================================================================

	public static BranchImpl createMasterBranch() {
		BranchMetadata masterBranchMetadata = BranchMetadata.createMasterBranchMetadata();
		return new BranchImpl(masterBranchMetadata, null);
	}

	public static BranchImpl createBranch(final BranchMetadata metadata, final Branch parentBranch) {
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		checkNotNull(parentBranch, "Precondition violation - argument 'parentBranch' must not be NULL!");
		return new BranchImpl(metadata, parentBranch);
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final BranchMetadata metadata;
	private final Branch origin;
	private TemporalKeyValueStore tkvs;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected BranchImpl(final BranchMetadata metadata, final Branch origin) {
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		// if we have an origin, its name must match the one stored in the metadata
		if (origin != null) {
			checkArgument(origin.getName().equals(metadata.getParentName()),
					"Precondition violation - argument 'origin' references a different name "
							+ "than the parent branch name in the branch metadata!");
		}
		this.metadata = metadata;
		this.origin = origin;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public String getName() {
		return this.metadata.getName();
	}

	@Override
	public Branch getOrigin() {
		return this.origin;
	}

	@Override
	public long getBranchingTimestamp() {
		return this.metadata.getBranchingTimestamp();
	}

	@Override
	public List<Branch> getOriginsRecursive() {
		if (this.origin == null) {
			// we are the master branch; by definition, we return an empty list (see JavaDoc).
			return Lists.newArrayList();
		} else {
			// we are not the master branch. Ask the origin to create the list for us
			List<Branch> origins = this.getOrigin().getOriginsRecursive();
			// ... and add our immediate parent to it.
			origins.add(this.getOrigin());
			return origins;
		}
	}

	@Override
	public long getNow() {
		if (this.origin != null) {
			// on a non-master branch, take the maximum of last commit and branching timestamp
			return Math.max(this.getBranchingTimestamp(), this.tkvs.getNow());
		} else {
			// on the master branch, only the timestamp of the last successful commit decides.
			return this.tkvs.getNow();
		}
	}

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.metadata == null ? 0 : this.metadata.hashCode());
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
		BranchImpl other = (BranchImpl) obj;
		if (this.metadata == null) {
			if (other.metadata != null) {
				return false;
			}
		} else if (!this.metadata.equals(other.metadata)) {
			return false;
		}
		return true;
	}

	// =====================================================================================================================
	// TO STRING
	// =====================================================================================================================

	@Override
	public String toString() {
		String originName = "NULL";
		if (this.origin != null) {
			originName = this.origin.getName();
		}
		return "Branch['" + this.getName() + "' origin='" + originName + "' branchingTimestamp="
				+ this.getBranchingTimestamp() + "]";
	}

	// =====================================================================================================================
	// INTERNAL API
	// =====================================================================================================================

	@Override
	public BranchMetadata getMetadata() {
		return this.metadata;
	}

	@Override
	public TemporalKeyValueStore getTemporalKeyValueStore() {
		return this.tkvs;
	}

	@Override
	public void setTemporalKeyValueStore(final TemporalKeyValueStore tkvs) {
		checkNotNull(tkvs, "Precondition violation - argument 'tkvs' must not be NULL!");
		this.tkvs = tkvs;
	}

}
