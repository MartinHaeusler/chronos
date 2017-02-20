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
	public void reindex(final String indexName) {
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			this.getIndexManagerBackend().rebuildIndexOnAllChunks(indexName);
			this.setIndexClean(indexName);
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
