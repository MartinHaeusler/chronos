package org.chronos.chronodb.internal.impl.engines.inmemory;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.BranchImpl;
import org.chronos.chronodb.internal.impl.BranchMetadata;
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicCache;
import org.chronos.chronodb.internal.impl.engines.base.AbstractBranchManager;

import com.google.common.collect.Sets;

public class InMemoryBranchManager extends AbstractBranchManager {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final Map<String, BranchInternal> branchNameToBranch = new ConcurrentHashMap<>();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public InMemoryBranchManager(final ChronoDBInternal db) {
		super(db);
		this.createMasterBranch();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Set<String> getBranchNames() {
		return Collections.unmodifiableSet(Sets.newHashSet(this.branchNameToBranch.keySet()));
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	@Override
	protected BranchInternal getBranchInternal(final String name) {
		return this.branchNameToBranch.get(name);
	}

	@Override
	protected BranchInternal createBranch(final BranchMetadata metadata) {
		checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
		BranchInternal branch = BranchImpl.createBranch(metadata, this.getBranch(metadata.getParentName()));
		this.createTKVS(branch);
		this.branchNameToBranch.put(branch.getName(), branch);
		return branch;
	}

	protected BranchInternal createMasterBranch() {
		BranchImpl masterBranch = BranchImpl.createMasterBranch();
		this.createTKVS(masterBranch);
		this.branchNameToBranch.put(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, masterBranch);
		return masterBranch;
	}

	protected TemporalKeyValueStore createTKVS(final BranchInternal branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		TemporalKeyValueStore tkvs = new InMemoryTKVS(this.owningDb, branch);
		ChronoDBConfiguration config = this.owningDb.getConfiguration();
		if (config.isCachingEnabled()) {
			// caching is enabled, so we create the cache for the branch here
			ChronoDBCache cache = new MosaicCache(config.getCacheMaxSize());
			tkvs.setCache(cache);
		}
		return tkvs;
	}

}
