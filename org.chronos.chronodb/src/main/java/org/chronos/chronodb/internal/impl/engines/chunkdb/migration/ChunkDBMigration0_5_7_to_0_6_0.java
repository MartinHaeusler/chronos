package org.chronos.chronodb.internal.impl.engines.chunkdb.migration;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;
import org.chronos.chronodb.internal.impl.BranchMetadata;
import org.chronos.chronodb.internal.impl.BranchMetadata2;
import org.chronos.chronodb.internal.impl.IBranchMetadata;
import org.chronos.chronodb.internal.impl.engines.chunkdb.BranchMetadataFile;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChunkedChronoDB;
import org.chronos.chronodb.internal.impl.engines.tupl.BranchMetadataIndex;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.common.exceptions.ChronosIOException;

/**
 * This migration solves the problem of "unsafe string handling" for branch names, specifically for ChunkDB.
 *
 * <p>
 * In essence, it replaces all instances of the deprecated {@link BranchMetadata} class with the newer version {@link BranchMetadata2}, and uses the branch name as directory name for existing branches. All new branches will receive a UUID-like directory name.
 *
 * <p>
 * This migration also creates the <code>branchMetadata.properties</code> file that was introduced in 0.6.0 and contains the {@link IBranchMetadata} object in a property (key-value) format.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@SuppressWarnings("deprecation")
@Migration(from = "0.5.7", to = "0.6.0")
public class ChunkDBMigration0_5_7_to_0_6_0 implements ChronosMigration<ChunkedChronoDB> {

	@Override
	public void execute(final ChunkedChronoDB chronoDB) {
		checkNotNull(chronoDB, "Precondition violation - argument 'chronoDB' must not be NULL!");
		// we need to replace all branch metadata elements in the DB with the new versions.
		try (TuplTransaction tx = chronoDB.openTx()) {
			Set<IBranchMetadata> branchMetadata = BranchMetadataIndex.values(tx);
			branchMetadata.stream()
					// only consider "old" versions of the branch metadata class
					.filter(branch -> branch instanceof BranchMetadata)
					// migrate all old versions
					.map(branch -> this.migrateBranch(chronoDB, (BranchMetadata) branch))
					// ... and save them
					.forEach(newBranchMetadata -> {
						BranchMetadataIndex.insertOrUpdate(tx, newBranchMetadata);
					});
			tx.commit();
		}
		chronoDB.getBranchManager().reloadBranchMetadataFromStore();
		chronoDB.getChunkManager().reloadChunksFromDisk();
	}

	private IBranchMetadata migrateBranch(final ChunkedChronoDB db, final BranchMetadata oldVersion) {
		if (oldVersion instanceof BranchMetadata) {
			// migrate from the old version to the new one
			String name = oldVersion.getName();
			String parentName = oldVersion.getParentName();
			long branchingTimestamp = oldVersion.getBranchingTimestamp();
			// for all old versions, we keep using the branch name as directory name
			String directoryName = oldVersion.getName();
			IBranchMetadata newVersion = IBranchMetadata.create(name, parentName, branchingTimestamp, directoryName);
			// create the "branchMetadata.properties" file
			File rootFile = db.getConfiguration().getWorkingFile().getParentFile();
			File branchesDir = new File(rootFile, ChunkedChronoDB.FILENAME__BRANCHES_DIRECTORY);
			File branchDir = new File(branchesDir, oldVersion.getName());
			File metaFile = new File(branchDir, ChunkedChronoDB.FILENAME__BRANCH_METADATA_FILE);
			try {
				metaFile.createNewFile();
				BranchMetadataFile.write(newVersion, metaFile);
			} catch (IOException | ChronosIOException e) {
				throw new ChronosIOException("Migration failed: could not create branch metadata file! See root cause for details.", e);
			}
			return newVersion;
		} else {
			// keep it the way it is
			return oldVersion;
		}
	}

}
