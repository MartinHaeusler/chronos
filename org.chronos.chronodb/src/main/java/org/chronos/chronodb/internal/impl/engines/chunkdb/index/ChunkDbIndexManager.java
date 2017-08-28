package org.chronos.chronodb.internal.impl.engines.chunkdb.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.api.Lockable.LockHolder;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChunkedChronoDB;
import org.chronos.chronodb.internal.impl.index.DocumentBasedIndexManager;

public class ChunkDbIndexManager extends DocumentBasedIndexManager {

	public ChunkDbIndexManager(final ChunkedChronoDB owningDB) {
		super(owningDB, new ChunkDbIndexManagerBackend(owningDB));
	}

	@Override
	public void reindexAll() {
		// this is a more efficient implementation for the ChunkDB indexer than the superclass
		// can offer. This is due to the fact that when re-indexing a chunk, ALL indices are
		// rebuilt for better performance. It therefore makes no sense to iterate over the
		// individual indices and attempt to rebuild them one by one (as the superclass does).
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			if (this.getDirtyIndices().isEmpty()) {
				// no indices are dirty -> no need to re-index
				return;
			}
			this.getIndexManagerBackend().rebuildIndexOnAllChunks();
			for (String indexName : this.getIndexNames()) {
				this.setIndexClean(indexName);
			}
			this.getIndexManagerBackend().persistIndexDirtyStates(this.indexNameToDirtyFlag);
			this.clearQueryCache();
		}
	}

	@Override
	public ChunkedChronoDB getOwningDB() {
		return (ChunkedChronoDB) super.getOwningDB();
	}

	@Override
	public ChunkDbIndexManagerBackend getIndexManagerBackend() {
		return (ChunkDbIndexManagerBackend) super.getIndexManagerBackend();
	}

	public void reindexHeadRevision(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.getIndexManagerBackend().rebuildIndexOnHeadChunk(branchName);
	}

}
