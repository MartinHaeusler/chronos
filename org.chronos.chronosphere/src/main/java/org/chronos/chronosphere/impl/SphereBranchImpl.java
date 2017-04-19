package org.chronos.chronosphere.impl;

import static com.google.common.base.Preconditions.*;

import java.util.List;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronosphere.api.SphereBranch;

import com.google.common.collect.Lists;

public class SphereBranchImpl implements SphereBranch {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	public static SphereBranchImpl createMasterBranch(final GraphBranch backingMasterBranch) {
		checkNotNull(backingMasterBranch, "Precondition violation - argument 'backingMasterBranch' must not be NULL!");
		checkArgument(backingMasterBranch.getName().equals(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER), "Precondition violation - the backing branch is not the master branch!");
		checkArgument(backingMasterBranch.getOrigin() == null, "Precondition violation - the backing branch is not the master branch!");
		return new SphereBranchImpl(backingMasterBranch, null);
	}

	public static SphereBranchImpl createBranch(final GraphBranch backingBranch, final SphereBranch origin) {
		checkNotNull(backingBranch, "Precondition violation - argument 'backingMasterBranch' must not be NULL!");
		checkNotNull(origin, "Precondition violation - argument 'origin' must not be NULL!");
		checkArgument(backingBranch.getOrigin().getName().equals(origin.getName()), "Precondition violation - the arguments do not refer to the same branch!");
		return new SphereBranchImpl(backingBranch, origin);
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final GraphBranch backingBranch;
	private final SphereBranch origin;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected SphereBranchImpl(final GraphBranch backingBranch, final SphereBranch origin) {
		checkNotNull(backingBranch, "Precondition violation - argument 'backingBranch' must not be NULL!");
		this.backingBranch = backingBranch;
		this.origin = origin;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public String getName() {
		return this.backingBranch.getName();
	}

	@Override
	public SphereBranch getOrigin() {
		return this.origin;
	}

	@Override
	public long getBranchingTimestamp() {
		return this.backingBranch.getBranchingTimestamp();
	}

	@Override
	public List<SphereBranch> getOriginsRecursive() {
		if (this.origin == null) {
			// we are the master branch; by definition, we return an empty list (see JavaDoc).
			return Lists.newArrayList();
		} else {
			// we are not the master branch. Ask the origin to create the list for us
			List<SphereBranch> origins = this.getOrigin().getOriginsRecursive();
			// ... and add our immediate parent to it.
			origins.add(this.getOrigin());
			return origins;
		}
	}

	@Override
	public long getNow() {
		return this.backingBranch.getNow();
	}

	// =====================================================================================================================
	// HASH CODE & EQUALS
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.backingBranch == null ? 0 : this.backingBranch.hashCode());
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
		SphereBranchImpl other = (SphereBranchImpl) obj;
		if (this.backingBranch == null) {
			if (other.backingBranch != null) {
				return false;
			}
		} else if (!this.backingBranch.equals(other.backingBranch)) {
			return false;
		}
		return true;
	}

}
