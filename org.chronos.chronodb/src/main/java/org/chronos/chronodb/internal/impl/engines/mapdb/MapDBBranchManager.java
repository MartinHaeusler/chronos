package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;
import static org.chronos.common.logging.ChronoLogger.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.BranchImpl;
import org.chronos.chronodb.internal.impl.BranchMetadata;
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicCache;
import org.chronos.chronodb.internal.impl.engines.base.AbstractBranchManager;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MapDBBranchManager extends AbstractBranchManager implements BranchManager {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final String NAVIGATION_MAP_NAME = NavigationMap.NAME;
	public static final String BRANCH_METADATA_MAP_NAME = BranchMetadataMap.NAME;

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final Map<String, BranchInternal> loadedBranches = Maps.newConcurrentMap();
	private final Map<String, BranchMetadata> branchMetadata = Maps.newConcurrentMap();

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected MapDBBranchManager(final MapDBChronoDB owningDb) {
		super(owningDb);
		this.loadBranchMetadata();
		this.ensureMasterBranchExists();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Set<String> getBranchNames() {
		return Collections.unmodifiableSet(Sets.newHashSet(this.branchMetadata.keySet()));
	}

	@Override
	protected BranchInternal createBranch(final BranchMetadata metadata) {
		BranchInternal parentBranch = null;
		BranchImpl branch = null;
		if (metadata.getParentName() != null) {
			// regular branch
			parentBranch = (BranchInternal) this.getBranch(metadata.getParentName());
			branch = BranchImpl.createBranch(metadata, parentBranch);
		} else {
			// master branch
			parentBranch = null;
			branch = BranchImpl.createMasterBranch();
		}
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			String keyspaceName = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
			String tableName = MatrixMap.generateRandomName();
			logTrace("Creating branch: [" + branch.getName() + ", " + keyspaceName + ", " + tableName + "]");
			NavigationMap.insert(tx, branch.getName(), keyspaceName, tableName, 0L);
			BranchMetadataMap.insertOrUpdate(tx, branch.getMetadata());
			tx.commit();
		}
		this.attachTKVS(branch);
		this.loadedBranches.put(branch.getName(), branch);
		this.branchMetadata.put(branch.getName(), branch.getMetadata());
		return branch;
	}

	@Override
	protected BranchInternal getBranchInternal(final String name) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		BranchInternal branch = this.loadedBranches.get(name);
		if (branch != null) {
			// already loaded
			return branch;
		}
		// not loaded yet; load it
		BranchMetadata metadata = this.branchMetadata.get(name);
		if (metadata == null) {
			return null;
		}
		if (metadata.getParentName() == null) {
			// we are the master branch
			branch = BranchImpl.createMasterBranch();
		} else {
			Branch parentBranch = this.getBranch(metadata.getParentName());
			branch = BranchImpl.createBranch(metadata, parentBranch);
		}
		// attach the TKVS to the branch
		this.attachTKVS(branch);
		this.loadedBranches.put(branch.getName(), branch);
		return branch;
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private MapDBChronoDB getOwningDB() {
		return (MapDBChronoDB) this.owningDb;
	}

	private void ensureMasterBranchExists() {
		BranchMetadata masterBranchMetadata = BranchMetadata.createMasterBranchMetadata();
		if (this.existsBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)) {
			// we know that the master branch exists in our navigation map.
			// ensure that it also exists in the branch metadata map
			try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
				BranchMetadataMap.insertOrUpdate(tx, masterBranchMetadata);
			}
			return;
		}
		this.createBranch(masterBranchMetadata);
	}

	private void loadBranchMetadata() {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			Set<BranchMetadata> allMetadata = BranchMetadataMap.values(tx);
			for (BranchMetadata metadata : allMetadata) {
				this.branchMetadata.put(metadata.getName(), metadata);
			}
		}
	}

	private void attachTKVS(final BranchInternal branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		MapDBTkvs branchTKVS = new MapDBTkvs(this.getOwningDB(), branch);
		ChronoDBConfiguration config = this.getOwningDB().getConfiguration();
		if (config.isCachingEnabled()) {
			// caching is enabled, so we create the cache for the branch here
			ChronoDBCache cache = new MosaicCache(config.getCacheMaxSize());
			branchTKVS.setCache(cache);
		}
	}

}
