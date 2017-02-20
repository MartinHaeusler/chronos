package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.Lockable.LockHolder;
import org.chronos.chronodb.internal.api.index.IndexManagerBackend;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public abstract class AbstractBackendDelegatingIndexManager<C extends ChronoDBInternal, B extends IndexManagerBackend>
		extends AbstractIndexManager<C> {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final B backend;

	protected final SetMultimap<String, ChronoIndexer> indexNameToIndexers = HashMultimap.create();
	protected final Map<String, Boolean> indexNameToDirtyFlag = Maps.newHashMap();

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public AbstractBackendDelegatingIndexManager(final C owningDB, final B backend) {
		super(owningDB);
		checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
		this.backend = backend;
		// initialize the indexers by loading them from the backend
		SetMultimap<String, ChronoIndexer> loadedIndexers = this.backend.loadIndexersFromPersistence();
		this.indexNameToIndexers.putAll(loadedIndexers);
		this.indexNameToDirtyFlag.clear();
		this.indexNameToDirtyFlag.putAll(this.backend.loadIndexStates());
	}

	// =================================================================================================================
	// INDEX MANAGEMENT
	// =================================================================================================================

	@Override
	public Set<String> getIndexNames() {
		try (LockHolder lock = this.getOwningDB().lockNonExclusive()) {
			return Collections.unmodifiableSet(Sets.newHashSet(this.indexNameToIndexers.keySet()));
		}
	}

	@Override
	public Set<ChronoIndexer> getIndexers() {
		try (LockHolder lock = this.getOwningDB().lockNonExclusive()) {
			return Collections.unmodifiableSet(Sets.newHashSet(this.indexNameToIndexers.values()));
		}
	}

	@Override
	public Map<String, Set<ChronoIndexer>> getIndexersByIndexName() {
		try (LockHolder lock = this.getOwningDB().lockNonExclusive()) {
			Map<String, Collection<ChronoIndexer>> map = this.indexNameToIndexers.asMap();
			Map<String, Set<ChronoIndexer>> resultMap = Maps.newHashMap();
			for (Entry<String, Collection<ChronoIndexer>> entry : map.entrySet()) {
				String indexName = entry.getKey();
				Collection<ChronoIndexer> indexers = entry.getValue();
				Set<ChronoIndexer> indexerSet = Collections.unmodifiableSet(Sets.newHashSet(indexers));
				resultMap.put(indexName, indexerSet);
			}
			return Collections.unmodifiableMap(resultMap);
		}
	}

	@Override
	public boolean isReindexingRequired() {
		try (LockHolder lock = this.getOwningDB().lockNonExclusive()) {
			return this.indexNameToDirtyFlag.values().contains(true);
		}
	}

	@Override
	public Set<String> getDirtyIndices() {
		try (LockHolder lock = this.getOwningDB().lockNonExclusive()) {
			// create a stream on the entry set of the map
			Set<String> dirtyIndices = this.indexNameToDirtyFlag.entrySet().stream()
					// keep only the entries where the dirty flag is true
					.filter(entry -> entry.getValue() == true)
					// from each entry, keep only the key
					.map(entry -> entry.getKey())
					// put the keys in a set
					.collect(Collectors.toSet());
			return Collections.unmodifiableSet(dirtyIndices);
		}
	}

	@Override
	public void removeIndex(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			this.backend.deleteIndexAndIndexers(indexName);
			this.indexNameToIndexers.removeAll(indexName);
			this.clearQueryCache();
		}
	}

	@Override
	public void clearAllIndices() {
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			this.backend.deleteAllIndicesAndIndexers();
			this.indexNameToIndexers.clear();
			this.clearQueryCache();
		}
	}

	@Override
	public void addIndexer(final String indexName, final ChronoIndexer indexer) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			this.backend.persistIndexer(indexName, indexer);
			this.indexNameToIndexers.put(indexName, indexer);
			this.setIndexDirty(indexName);
			this.clearQueryCache();
		}
	}

	@Override
	public void removeIndexer(final ChronoIndexer indexer) {
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			Set<String> indexNamesContainingTheIndexer = Sets.newHashSet();
			for (String indexName : this.getIndexNames()) {
				if (this.indexNameToIndexers.get(indexName).contains(indexer)) {
					indexNamesContainingTheIndexer.add(indexName);
				}
			}
			for (String indexName : indexNamesContainingTheIndexer) {
				boolean removed = this.indexNameToIndexers.remove(indexName, indexer);
				if (removed) {
					this.setIndexDirty(indexName);
				}
			}
			// TODO PERFORMANCE: this is very inefficient on SQL backend. Can it be avoided?
			this.backend.persistIndexers(HashMultimap.create(this.indexNameToIndexers));
			this.clearQueryCache();
		}
	}

	// =====================================================================================================================
	// ROLLBACK METHODS
	// =====================================================================================================================

	@Override
	public void rollback(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			Set<String> branchNames = this.getOwningDB().getBranchManager().getBranchNames();
			this.backend.rollback(branchNames, timestamp);
			this.clearQueryCache();
		}
	}

	@Override
	public void rollback(final Branch branch, final long timestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			this.backend.rollback(Collections.singleton(branch.getName()), timestamp);
			this.clearQueryCache();
		}
	}

	@Override
	public void rollback(final Branch branch, final long timestamp, final Set<QualifiedKey> keys) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(keys, "Precondition violation - argument 'keys' must not be NULL!");
		try (LockHolder lock = this.getOwningDB().lockExclusive()) {
			this.backend.rollback(Collections.singleton(branch.getName()), timestamp, keys);
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	protected B getIndexManagerBackend() {
		return this.backend;
	}

	protected void setIndexDirty(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		Boolean previous = this.indexNameToDirtyFlag.put(indexName, true);
		if (previous == null || previous == false) {
			this.backend.persistIndexDirtyStates(this.indexNameToDirtyFlag);
		}
	}

	protected void setIndexClean(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		Boolean previous = this.indexNameToDirtyFlag.put(indexName, false);
		if (previous == null || previous == true) {
			this.backend.persistIndexDirtyStates(this.indexNameToDirtyFlag);
		}
	}
}
