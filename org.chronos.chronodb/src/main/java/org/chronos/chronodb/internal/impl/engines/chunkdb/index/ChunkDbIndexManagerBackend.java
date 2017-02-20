package org.chronos.chronodb.internal.impl.engines.chunkdb.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.ChronoIndexModifications;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronodb.internal.impl.engines.base.AbstractDocumentBasedIndexManagerBackend;
import org.chronos.chronodb.internal.impl.engines.chunkdb.BranchChunkManager;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChronoChunk;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChunkedChronoDB;
import org.chronos.chronodb.internal.impl.engines.tupl.TuplChronoDB;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class ChunkDbIndexManagerBackend extends AbstractDocumentBasedIndexManagerBackend {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	private static final String MANAGEMENT_INDEX__INDEXERS = "chronodb_indexers";
	private static final String MANAGEMENT_INDEX__DIRTY_FLAGS = "chronodb_indexdirty";

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	protected final IndexChunkManager indexChunkManager;

	protected final SetMultimap<String, ChronoIndexer> indexNameToIndexers = HashMultimap.create();
	protected final Map<String, Boolean> indexNameToDirtyFlag = Maps.newHashMap();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChunkDbIndexManagerBackend(final ChunkedChronoDB owningDB) {
		super(owningDB);
		this.indexChunkManager = new IndexChunkManager(owningDB);
	}

	// =================================================================================================================
	// ABSTRACT METHOD IMPLEMENTATIONS
	// =================================================================================================================

	@Override
	public void deleteIndexContents(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.indexChunkManager.deleteIndexContents(indexName);
	}

	@Override
	public void applyModifications(final ChronoIndexModifications indexModifications) {
		checkNotNull(indexModifications, "Precondition violation - argument 'indexModifications' must not be NULL!");
		// first, group the modifications by branch. Usually, all modifications should belong to the same branch,
		// we do this just to be safe.
		Map<String, ChronoIndexModifications> branchToModifications = indexModifications.groupByBranch();
		// Then, we iterate over all groups...
		for (Entry<String, ChronoIndexModifications> entry : branchToModifications.entrySet()) {
			String branchName = entry.getKey();
			ChronoIndexModifications branchIndexModifications = entry.getValue();
			// retrieve the chunk that represents the head revision of the branch (because that's the only place in each
			// branch
			// where modifications can occur)
			ChronoChunk headChunk = this.getOwningDB().getChunkManager().getChunkManagerForBranch(branchName)
					.getChunkForHeadRevision();
			// get the index for the head revision chunk
			DocumentBasedChunkIndex indexForChunk = this.indexChunkManager.getIndexForChunk(headChunk);
			// update it by applying the index modifications that belong to the branch
			indexForChunk.applyModifications(branchIndexModifications);
		}
	}

	@Override
	public Map<String, Map<String, ChronoIndexDocument>> getMatchingBranchLocalDocuments(
			final ChronoIdentifier chronoIdentifier) {
		checkNotNull(chronoIdentifier, "Precondition violation - argument 'chronoIdentifier' must not be NULL!");
		String branchName = chronoIdentifier.getBranchName();
		// fetch the chunk responsible for the given branch and timestamp
		ChronoChunk chunk = this.getOwningDB().getChunkManager().getChunkManagerForBranch(branchName)
				.getChunkForTimestamp(chronoIdentifier.getTimestamp());
		// fetch the index manager for this chunk
		DocumentBasedChunkIndex chunkIndex = this.indexChunkManager.getIndexForChunk(chunk);
		// forward the call
		return chunkIndex.getMatchingBranchLocalDocuments(chronoIdentifier);
	}

	@Override
	public SetMultimap<String, ChronoIndexer> loadIndexersFromPersistence() {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			return this.loadIndexersMap(tx);
		}
	}

	@Override
	public void persistIndexers(final SetMultimap<String, ChronoIndexer> indexNameToIndexers) {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			this.persistIndexersMap(indexNameToIndexers, tx);
			tx.commit();
		}
	}

	@Override
	public void persistIndexer(final String indexName, final ChronoIndexer indexer) {
		// TODO PERFORMANCE TUPL: Storing the entire map just to add one indexer is not very efficient.
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			SetMultimap<String, ChronoIndexer> map = this.loadIndexersMap(tx);
			map.put(indexName, indexer);
			this.persistIndexersMap(map, tx);
			this.indexChunkManager.addIndexer(indexName, indexer);
			tx.commit();
		}
	}

	@Override
	public void deleteIndexAndIndexers(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		// first, delete the indexers
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			SetMultimap<String, ChronoIndexer> indexersMap = this.loadIndexersFromPersistence();
			indexersMap.removeAll(indexName);
			this.persistIndexers(indexersMap);
			tx.commit();
		}
		// we do not immediately cleanup current and old chunks; specific index is not written/updated from now on
	}

	@Override
	public void deleteAllIndicesAndIndexers() {
		// first, delete the indexers
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			SetMultimap<String, ChronoIndexer> indexersMap = HashMultimap.create();
			this.persistIndexers(indexersMap);
			tx.commit();
		}
		// delete all index chunks
		this.indexChunkManager.deleteAllChunkIndices();
	}

	@Override
	public Map<String, Boolean> loadIndexStates() {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			byte[] dirtyFlagsSerialized = this.getIndexDirtyFlagsSerialForm(tx);
			Map<String, Boolean> map = this.deserializeObject(dirtyFlagsSerialized);
			if (map == null) {
				return Maps.newHashMap();
			} else {
				return map;
			}
		}
	}

	@Override
	public void persistIndexDirtyStates(final Map<String, Boolean> indexNameToDirtyFlag) {
		try (TuplTransaction tx = this.getOwningDB().openTx()) {
			byte[] serializedForm = this.serializeObject(indexNameToDirtyFlag);
			this.saveIndexDirtyFlags(tx, serializedForm);
			tx.commit();
		}
	}

	@Override
	protected Set<ChronoIndexDocument> getDocumentsTouchedAtOrAfterTimestamp(final long timestamp,
			final Set<String> branches) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		Set<String> branchNames = Sets.newHashSet();
		if (branches == null) {
			// "null" means "all branches"
			branchNames.addAll(this.getOwningDB().getBranchManager().getBranchNames());
		} else {
			branchNames.addAll(branches);
		}
		if (branchNames.isEmpty()) {
			// no branches -> empty result set
			return Sets.newHashSet();
		}
		Set<ChronoIndexDocument> resultSet = Sets.newHashSet();
		for (String branchName : branchNames) {
			// get the chunk for the branch name and request timestamp
			ChronoChunk chunk = this.getOwningDB().getChunkManager().getChunkManagerForBranch(branchName)
					.getChunkForTimestamp(timestamp);
			DocumentBasedChunkIndex indexForChunk = this.indexChunkManager.getIndexForChunk(chunk);
			Set<ChronoIndexDocument> branchDocs = indexForChunk.getDocumentsTouchedAtOrAfterTimestamp(timestamp,
					branchNames);
			resultSet.addAll(branchDocs);
		}
		return resultSet;
	}

	@Override
	protected Collection<ChronoIndexDocument> getTerminatedBranchLocalDocuments(final long timestamp,
			final String branchName, final SearchSpecification searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		// get the data chunk
		ChronoChunk chunk = this.getOwningDB().getChunkManager().getChunkManagerForBranch(branchName)
				.getChunkForTimestamp(timestamp);
		// get the corresponding index
		DocumentBasedChunkIndex index = this.indexChunkManager.getIndexForChunk(chunk);
		// forward the call
		return index.getTerminatedBranchLocalDocuments(timestamp, branchName, searchSpec);
	}

	@Override
	protected Collection<ChronoIndexDocument> getMatchingBranchLocalDocuments(final long timestamp,
			final String branchName, final SearchSpecification searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		// get the data chunk
		ChronoChunk chunk = this.getOwningDB().getChunkManager().getChunkManagerForBranch(branchName)
				.getChunkForTimestamp(timestamp);
		// get the corresponding index
		DocumentBasedChunkIndex index = this.indexChunkManager.getIndexForChunk(chunk);
		// forward the call
		return index.getMatchingBranchLocalDocuments(timestamp, branchName, searchSpec);
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	protected ChunkedChronoDB getOwningDB() {
		return (ChunkedChronoDB) this.owningDB;
	}

	private byte[] getIndexersSerialForm(final TuplTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		String indexName = TuplChronoDB.MANAGEMENT_INDEX_NAME;
		String key = MANAGEMENT_INDEX__INDEXERS + "_" + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
		return tx.load(indexName, key);
	}

	private void saveIndexers(final TuplTransaction tx, final byte[] serialForm) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(serialForm, "Precondition violation - argument 'serialForm' must not be NULL!");
		String indexName = TuplChronoDB.MANAGEMENT_INDEX_NAME;
		String key = MANAGEMENT_INDEX__INDEXERS + "_" + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
		tx.store(indexName, key, serialForm);
	}

	private byte[] getIndexDirtyFlagsSerialForm(final TuplTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		String indexName = TuplChronoDB.MANAGEMENT_INDEX_NAME;
		String key = MANAGEMENT_INDEX__DIRTY_FLAGS + "_" + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
		return tx.load(indexName, key);
	}

	private void saveIndexDirtyFlags(final TuplTransaction tx, final byte[] serialForm) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkNotNull(serialForm, "Precondition violation - argument 'serialForm' must not be NULL!");
		String indexName = TuplChronoDB.MANAGEMENT_INDEX_NAME;
		String key = MANAGEMENT_INDEX__DIRTY_FLAGS + "_" + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
		tx.store(indexName, key, serialForm);
	}

	private SetMultimap<String, ChronoIndexer> loadIndexersMap(final TuplTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		byte[] serializedForm = this.getIndexersSerialForm(tx);
		// Kryo doesn't like to convert the SetMultimap class directly, so we transform
		// it into a regular hash map with sets as values.
		Map<String, Set<ChronoIndexer>> map = this.deserializeObject(serializedForm);
		if (map == null) {
			return HashMultimap.create();
		} else {
			// we need to convert our internal map representation back into its multimap form
			SetMultimap<String, ChronoIndexer> multiMap = HashMultimap.create();
			for (Entry<String, Set<ChronoIndexer>> entry : map.entrySet()) {
				for (ChronoIndexer indexer : entry.getValue()) {
					multiMap.put(entry.getKey(), indexer);
				}
			}
			return multiMap;
		}
	}

	private void persistIndexersMap(final SetMultimap<String, ChronoIndexer> indexNameToIndexers,
			final TuplTransaction tx) {
		checkNotNull(indexNameToIndexers, "Precondition violation - argument 'indexNameToIndexers' must not be NULL!");
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		// Kryo doesn't like to convert the SetMultimap class directly, so we transform
		// it into a regular hash map with sets as values.
		Map<String, Set<ChronoIndexer>> persistentMap = Maps.newHashMap();
		// we need to transform the multimap into an internal representation using a normal hash map.
		for (Entry<String, ChronoIndexer> entry : indexNameToIndexers.entries()) {
			Set<ChronoIndexer> set = persistentMap.get(entry.getKey());
			if (set == null) {
				set = Sets.newHashSet();
			}
			set.add(entry.getValue());
			persistentMap.put(entry.getKey(), set);
		}
		// first, serialize the indexers map to a binary format
		byte[] serialForm = this.serializeObject(persistentMap);
		// store the binary format in the database
		this.saveIndexers(tx, serialForm);
	}

	private <T> byte[] serializeObject(final T object) {
		return this.getOwningDB().getSerializationManager().serialize(object);
	}

	@SuppressWarnings("unchecked")
	private <T> T deserializeObject(final byte[] serializedForm) {
		if (serializedForm == null || serializedForm.length <= 0) {
			return null;
		}
		return (T) this.getOwningDB().getSerializationManager().deserialize(serializedForm);
	}

	protected void setIndexDirty(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		Boolean previous = this.indexNameToDirtyFlag.put(indexName, true);
		if (previous == null || previous == false) {
			this.persistIndexDirtyStates(this.indexNameToDirtyFlag);
		}
	}

	protected void setIndexClean(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		Boolean previous = this.indexNameToDirtyFlag.put(indexName, false);
		if (previous == null || previous == true) {
			this.persistIndexDirtyStates(this.indexNameToDirtyFlag);
		}
	}

	public void rebuildIndexOnAllChunks(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.deleteIndexContents(indexName);
		// iterate over all branches
		for (String branch : this.getOwningDB().getBranchManager().getBranchNames()) {
			BranchChunkManager branchChunkManager = this.getOwningDB().getChunkManager()
					.getOrCreateChunkManagerForBranch(branch);
			// iterate over all chunks
			List<ChronoChunk> chunks = branchChunkManager.getChunksForPeriod(Period.createOpenEndedRange(0));
			for (ChronoChunk chunk : chunks) {
				this.indexChunkManager.getIndexForChunk(chunk);
			}
		}
	}

	public void rebuildIndexOnHeadChunk(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		BranchChunkManager branchChunkManager = this.getOwningDB().getChunkManager()
				.getOrCreateChunkManagerForBranch(branchName);
		this.indexChunkManager.deleteIndexForChunk(branchChunkManager.getChunkForHeadRevision());
		this.indexChunkManager.getIndexForChunk(branchChunkManager.getChunkForHeadRevision());
	}

}
