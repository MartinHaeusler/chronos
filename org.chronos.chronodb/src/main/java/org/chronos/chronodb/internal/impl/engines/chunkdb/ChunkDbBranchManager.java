package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static org.chronos.common.logging.ChronoLogger.*;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.impl.BranchImpl;
import org.chronos.chronodb.internal.impl.BranchMetadata;
import org.chronos.chronodb.internal.impl.MatrixUtils;
import org.chronos.chronodb.internal.impl.engines.base.AbstractBranchManager;
import org.chronos.chronodb.internal.impl.engines.tupl.BranchMetadataIndex;
import org.chronos.chronodb.internal.impl.engines.tupl.NavigationIndex;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ChunkDbBranchManager extends AbstractBranchManager {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final Map<String, BranchInternal> loadedBranches = Maps.newConcurrentMap();
	private final Map<String, BranchMetadata> branchMetadata = Maps.newConcurrentMap();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected ChunkDbBranchManager(final ChunkedChronoDB owningDb) {
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
		this.getOwningDB().getChunkManager().getOrCreateChunkManagerForBranch(branch.getName());
		try (TuplTransaction tx = this.openTx()) {
			String keyspaceName = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
			String tableName = MatrixUtils.generateRandomName();
			logTrace("Creating branch: [" + branch.getName() + ", " + keyspaceName + ", " + tableName + "]");
			NavigationIndex.insert(tx, branch.getName(), keyspaceName, tableName, 0L);
			BranchMetadataIndex.insertOrUpdate(tx, branch.getMetadata());
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

	protected ChunkedChronoDB getOwningDB() {
		return (ChunkedChronoDB) this.owningDb;
	}

	protected TuplTransaction openTx() {
		return this.getOwningDB().openTx();
	}

	private void ensureMasterBranchExists() {
		BranchMetadata masterBranchMetadata = BranchMetadata.createMasterBranchMetadata();
		if (this.existsBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)) {
			// we know that the master branch exists in our navigation map.
			// ensure that it also exists in the branch metadata map
			try (TuplTransaction tx = this.openTx()) {
				BranchMetadataIndex.insertOrUpdate(tx, masterBranchMetadata);
				tx.commit();
			}
			return;
		}
		this.createBranch(masterBranchMetadata);
	}

	private void loadBranchMetadata() {
		try (TuplTransaction tx = this.openTx()) {
			Set<BranchMetadata> allMetadata = BranchMetadataIndex.values(tx);
			for (BranchMetadata metadata : allMetadata) {
				this.branchMetadata.put(metadata.getName(), metadata);
			}
			tx.commit();
		}
	}

	private ChunkDbTkvs attachTKVS(final BranchInternal branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return new ChunkDbTkvs(this.getOwningDB(), branch);
	}

}
