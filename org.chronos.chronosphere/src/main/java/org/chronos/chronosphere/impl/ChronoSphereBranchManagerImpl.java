package org.chronos.chronosphere.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronograph.api.branch.ChronoGraphBranchManager;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.api.ChronoSphereBranchManager;
import org.chronos.chronosphere.api.SphereBranch;

import com.google.common.collect.Maps;

public class ChronoSphereBranchManagerImpl implements ChronoSphereBranchManager {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final ChronoGraph graph;
	private final Map<GraphBranch, SphereBranch> backingBranchToSphereBranch;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronoSphereBranchManagerImpl(final ChronoGraph graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.graph = graph;
		this.backingBranchToSphereBranch = Maps.newHashMap();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public SphereBranch createBranch(final String branchName) {
		GraphBranch branch = this.getChronoGraphBranchManager().createBranch(branchName);
		return this.getOrCreateSphereBranch(branch);
	}

	@Override
	public SphereBranch createBranch(final String branchName, final long branchingTimestamp) {
		GraphBranch branch = this.getChronoGraphBranchManager().createBranch(branchName, branchingTimestamp);
		return this.getOrCreateSphereBranch(branch);
	}

	@Override
	public SphereBranch createBranch(final String parentName, final String newBranchName) {
		GraphBranch branch = this.getChronoGraphBranchManager().createBranch(parentName, newBranchName);
		return this.getOrCreateSphereBranch(branch);
	}

	@Override
	public SphereBranch createBranch(final String parentName, final String newBranchName, final long branchingTimestamp) {
		GraphBranch branch = this.getChronoGraphBranchManager().createBranch(parentName, newBranchName, branchingTimestamp);
		return this.getOrCreateSphereBranch(branch);
	}

	@Override
	public boolean existsBranch(final String branchName) {
		return this.getChronoGraphBranchManager().existsBranch(branchName);
	}

	@Override
	public SphereBranch getBranch(final String branchName) {
		GraphBranch backingBranch = this.getChronoGraphBranchManager().getBranch(branchName);
		return this.getOrCreateSphereBranch(backingBranch);
	}

	@Override
	public Set<String> getBranchNames() {
		return this.getChronoGraphBranchManager().getBranchNames();
	}

	@Override
	public Set<SphereBranch> getBranches() {
		Set<GraphBranch> branches = this.getChronoGraphBranchManager().getBranches();
		return branches.stream()
				// map each backing branch to a graph branch
				.map(backingBranch -> this.getOrCreateSphereBranch(backingBranch))
				// return the result as a set
				.collect(Collectors.toSet());
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private ChronoGraphBranchManager getChronoGraphBranchManager() {
		return this.graph.getBranchManager();
	}

	private SphereBranch getOrCreateSphereBranch(final GraphBranch backingBranch) {
		checkNotNull(backingBranch, "Precondition violation - argument 'backingBranch' must not be NULL!");
		// check if we already know that branch...
		SphereBranch graphBranch = this.backingBranchToSphereBranch.get(backingBranch);
		if (graphBranch != null) {
			// use the cached instance
			return graphBranch;
		}
		// we don't know that branch yet; construct it
		if (backingBranch.getOrigin() == null) {
			// no origin -> we are dealing with the master branch
			graphBranch = SphereBranchImpl.createMasterBranch(backingBranch);
		} else {
			// regular branch
			SphereBranch originBranch = this.getOrCreateSphereBranch(backingBranch.getOrigin());
			graphBranch = SphereBranchImpl.createBranch(backingBranch, originBranch);
		}
		// remember the graph branch in our cache
		this.backingBranchToSphereBranch.put(backingBranch, graphBranch);
		return graphBranch;
	}

}
