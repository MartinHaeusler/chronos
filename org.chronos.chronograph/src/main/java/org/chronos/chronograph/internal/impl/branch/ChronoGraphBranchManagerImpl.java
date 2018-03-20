package org.chronos.chronograph.internal.impl.branch;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronograph.api.branch.ChronoGraphBranchManager;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;

import com.google.common.collect.Maps;

public class ChronoGraphBranchManagerImpl implements ChronoGraphBranchManager {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final ChronoGraphInternal graph;
	private final Map<Branch, GraphBranch> backingBranchToGraphBranch;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronoGraphBranchManagerImpl(final ChronoGraphInternal graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.graph = graph;
		this.backingBranchToGraphBranch = Maps.newHashMap();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public GraphBranch createBranch(final String branchName) {
		Branch branch = this.getChronoDBBranchManager().createBranch(branchName);
		return this.getOrCreateGraphBranch(branch);
	}

	@Override
	public GraphBranch createBranch(final String branchName, final long branchingTimestamp) {
		Branch branch = this.getChronoDBBranchManager().createBranch(branchName, branchingTimestamp);
		return this.getOrCreateGraphBranch(branch);
	}

	@Override
	public GraphBranch createBranch(final String parentName, final String newBranchName) {
		Branch branch = this.getChronoDBBranchManager().createBranch(parentName, newBranchName);
		return this.getOrCreateGraphBranch(branch);
	}

	@Override
	public GraphBranch createBranch(final String parentName, final String newBranchName,
			final long branchingTimestamp) {
		Branch branch = this.getChronoDBBranchManager().createBranch(parentName, newBranchName, branchingTimestamp);
		return this.getOrCreateGraphBranch(branch);
	}

	@Override
	public boolean existsBranch(final String branchName) {
		return this.getChronoDBBranchManager().existsBranch(branchName);
	}

	@Override
	public GraphBranch getBranch(final String branchName) {
		Branch backingBranch = this.getChronoDBBranchManager().getBranch(branchName);
		return this.getOrCreateGraphBranch(backingBranch);
	}

	@Override
	public Set<String> getBranchNames() {
		return this.getChronoDBBranchManager().getBranchNames();
	}

	@Override
	public Set<GraphBranch> getBranches() {
		Set<Branch> branches = this.getChronoDBBranchManager().getBranches();
		return branches.stream()
				// map each backing branch to a graph branch
				.map(backingBranch -> this.getOrCreateGraphBranch(backingBranch))
				// return the result as a set
				.collect(Collectors.toSet());
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private BranchManager getChronoDBBranchManager() {
		return this.graph.getBackingDB().getBranchManager();
	}

	private GraphBranch getOrCreateGraphBranch(final Branch backingBranch) {
		checkNotNull(backingBranch, "Precondition violation - argument 'backingBranch' must not be NULL!");
		// check if we already know that branch...
		GraphBranch graphBranch = this.backingBranchToGraphBranch.get(backingBranch);
		if (graphBranch != null) {
			// use the cached instance
			return graphBranch;
		}
		// we don't know that branch yet; construct it
		if (backingBranch.getOrigin() == null) {
			// no origin -> we are dealing with the master branch
			graphBranch = GraphBranchImpl.createMasterBranch(backingBranch);
		} else {
			// regular branch
			GraphBranch originBranch = this.getOrCreateGraphBranch(backingBranch.getOrigin());
			graphBranch = GraphBranchImpl.createBranch(backingBranch, originBranch);
		}
		// remember the graph branch in our cache
		this.backingBranchToGraphBranch.put(backingBranch, graphBranch);
		return graphBranch;
	}

}
