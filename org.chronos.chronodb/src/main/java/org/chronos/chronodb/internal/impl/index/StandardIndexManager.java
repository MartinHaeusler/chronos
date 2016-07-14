package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.exceptions.ChronoDBQuerySyntaxException;
import org.chronos.chronodb.api.exceptions.IndexerConflictException;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.ChronoIndexModifications;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.engines.base.IndexManagerBackend;
import org.chronos.chronodb.internal.impl.index.querycache.ChronoIndexQueryCache;
import org.chronos.chronodb.internal.impl.index.querycache.LRUIndexQueryCache;
import org.chronos.chronodb.internal.impl.index.querycache.NoIndexQueryCache;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class StandardIndexManager implements IndexManager {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final ChronoDBInternal owningDB;
	private final IndexManagerBackend backend;

	private final ChronoIndexQueryCache queryCache;

	private final SetMultimap<String, ChronoIndexer> indexNameToIndexers = HashMultimap.create();
	private final Map<String, Boolean> indexNameToDirtyFlag = Maps.newHashMap();

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public StandardIndexManager(final ChronoDBInternal owningDB, final IndexManagerBackend backend) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
		this.owningDB = owningDB;
		this.backend = backend;
		// initialize the indexers by loading them from the backend
		SetMultimap<String, ChronoIndexer> loadedIndexers = this.backend.loadIndexersFromPersistence();
		this.indexNameToIndexers.putAll(loadedIndexers);
		this.indexNameToDirtyFlag.clear();
		this.indexNameToDirtyFlag.putAll(this.backend.loadIndexStates());
		// check configuration to see if we want to have query caching
		ChronoDBConfiguration chronoDbConfig = this.owningDB.getConfiguration();
		if (chronoDbConfig.isIndexQueryCachingEnabled()) {
			int maxIndexQueryCacheSize = chronoDbConfig.getIndexQueryCacheMaxSize();
			boolean debugModeEnabled = chronoDbConfig.isDebugModeEnabled();
			this.queryCache = new LRUIndexQueryCache(maxIndexQueryCacheSize, debugModeEnabled);
		} else {
			// according to the configuration, no caching is required. To make sure that we still have
			// the same object structure (i.e. we don't have to deal with the cache object being NULL),
			// we create a pseudo-cache instead that actually "caches" nothing.
			this.queryCache = new NoIndexQueryCache();
		}
	}

	// =================================================================================================================
	// INDEX MANAGEMENT
	// =================================================================================================================

	@Override
	public Set<String> getIndexNames() {
		return this.owningDB.performNonExclusive(() -> {
			return Collections.unmodifiableSet(Sets.newHashSet(this.indexNameToIndexers.keySet()));
		});
	}

	@Override
	public void removeIndex(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.owningDB.performExclusive(() -> {
			this.backend.deleteIndexAndIndexers(indexName);
			this.indexNameToIndexers.removeAll(indexName);
			this.clearQueryCache();
		});
	}

	@Override
	public void clearAllIndices() {
		this.owningDB.performExclusive(() -> {
			this.backend.deleteAllIndicesAndIndexers();
			this.indexNameToIndexers.clear();
			this.clearQueryCache();
		});
	}

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	@Override
	public void addIndexer(final String indexName, final ChronoIndexer indexer) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
		this.owningDB.performExclusive(() -> {
			this.backend.persistIndexer(indexName, indexer);
			this.indexNameToIndexers.put(indexName, indexer);
			this.setIndexDirty(indexName);
			this.clearQueryCache();
		});
	}

	@Override
	public void removeIndexer(final ChronoIndexer indexer) {
		this.owningDB.performExclusive(() -> {
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
		});
	}

	@Override
	public Set<ChronoIndexer> getIndexers() {
		return this.owningDB.performNonExclusive(() -> {
			return Collections.unmodifiableSet(Sets.newHashSet(this.indexNameToIndexers.values()));
		});
	}

	@Override
	public Map<String, Set<ChronoIndexer>> getIndexersByIndexName() {
		return this.owningDB.performNonExclusive(() -> {
			Map<String, Collection<ChronoIndexer>> map = this.indexNameToIndexers.asMap();
			Map<String, Set<ChronoIndexer>> resultMap = Maps.newHashMap();
			for (Entry<String, Collection<ChronoIndexer>> entry : map.entrySet()) {
				String indexName = entry.getKey();
				Collection<ChronoIndexer> indexers = entry.getValue();
				Set<ChronoIndexer> indexerSet = Collections.unmodifiableSet(Sets.newHashSet(indexers));
				resultMap.put(indexName, indexerSet);
			}
			return Collections.unmodifiableMap(resultMap);
		});
	}

	// =================================================================================================================
	// INDEXING METHODS
	// =================================================================================================================

	@Override
	public void reindex(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkArgument(this.getIndexNames().contains(indexName),
				"Precondition violation - argument 'indexName' does not refer to a known index!");
		this.owningDB.performExclusive(() -> {
			// first, delete whatever is in that index
			this.backend.deleteIndexContents(indexName);
			// then, iterate over the contents of the database
			BranchManager branchManager = this.owningDB.getBranchManager();
			Set<Branch> branches = branchManager.getBranches();
			Map<ChronoIdentifier, Pair<Object, Object>> identifierToValue = Maps.newHashMap();
			SerializationManager serializationManager = this.owningDB.getSerializationManager();
			for (Branch branch : branches) {
				TemporalKeyValueStore tkvs = ((BranchInternal) branch).getTemporalKeyValueStore();
				long now = tkvs.getNow();
				try (CloseableIterator<ChronoDBEntry> entries = tkvs.allEntriesIterator(now)) {
					while (entries.hasNext()) {
						ChronoDBEntry entry = entries.next();
						ChronoIdentifier identifier = entry.getIdentifier();
						byte[] value = entry.getValue();
						Object deserializedValue = null;
						if (value != null && value.length > 0) {
							// only deserialize if the stored value is non-null
							deserializedValue = serializationManager.deserialize(value);
						}
						ChronoDBTransaction historyTx = tkvs.tx(branch.getName(), identifier.getTimestamp() - 1);
						Object historyValue = historyTx.get(identifier.getKeyspace(), identifier.getKey());
						identifierToValue.put(identifier, Pair.of(historyValue, deserializedValue));
					}
				}
			}
			this.index(identifierToValue);
			// the index has been cleared. Remove the dirty flag
			this.setIndexClean(indexName);
			// clear the query cache
			this.clearQueryCache();
		});
	}

	@Override
	public void reindexAll() {
		this.owningDB.performExclusive(() -> {
			Map<String, Boolean> copyMap = Maps.newHashMap(this.indexNameToDirtyFlag);
			for (Entry<String, Boolean> entry : copyMap.entrySet()) {
				String indexName = entry.getKey();
				Boolean dirtyFlag = entry.getValue();
				if (dirtyFlag) {
					this.reindex(indexName);
				}
			}
			this.backend.persistIndexDirtyStates(this.indexNameToDirtyFlag);
			this.clearQueryCache();
		});
	}

	@Override
	public void index(final Map<ChronoIdentifier, Pair<Object, Object>> identifierToOldAndNewValue) {
		checkNotNull(identifierToOldAndNewValue,
				"Precondition violation - argument 'identifierToOldAndNewValue' must not be NULL!");
		if (identifierToOldAndNewValue.isEmpty()) {
			// no workload to index
			return;
		}
		if (StandardIndexManager.this.getIndexNames().isEmpty()) {
			// no indices registered
			return;
		}
		this.owningDB.performNonExclusive(() -> {
			new IndexingProcess().index(identifierToOldAndNewValue);
		});
	}

	@Override
	public boolean isReindexingRequired() {
		return this.owningDB.performNonExclusive(() -> {
			return this.indexNameToDirtyFlag.values().contains(true);
		});
	}

	@Override
	public Set<String> getDirtyIndices() {
		return this.owningDB.performNonExclusive(() -> {
			// create a stream on the entry set of the map
			Set<String> dirtyIndices = this.indexNameToDirtyFlag.entrySet().stream()
					// keep only the entries where the dirty flag is true
					.filter(entry -> entry.getValue() == true)
					// from each entry, keep only the key
					.map(entry -> entry.getKey())
					// put the keys in a set
					.collect(Collectors.toSet());
			return Collections.unmodifiableSet(dirtyIndices);
		});
	}

	// =================================================================================================================
	// INDEX QUERY METHODS
	// =================================================================================================================

	@Override
	public Set<ChronoIdentifier> queryIndex(final long timestamp, final Branch branch,
			final SearchSpecification searchSpec) {
		String property = searchSpec.getProperty();
		if (this.getIndexNames().contains(property) == false) {
			throw new UnknownIndexException("There is no index named '" + property + "'!");
		}
		if (this.queryCache == null) {
			// cache disabled, return the result of the request directly
			return this.performIndexQuery(timestamp, branch, searchSpec);
		} else {
			// cache enabled, pipe the request through the cache
			return this.queryCache.getOrCalculate(timestamp, branch, searchSpec, () -> {
				return this.performIndexQuery(timestamp, branch, searchSpec);
			});
		}
	}

	@Override
	public Iterator<QualifiedKey> evaluate(final long timestamp, final Branch branch, final ChronoDBQuery query) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must be >= 0!");
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		return this.owningDB.performNonExclusive(() -> {
			// walk the AST of the query in a bottom-up fashion, applying the following strategy:
			// - WHERE node: run the query and remember the result set
			// - AND node: perform set intersection of left and right child result sets
			// - OR node: perform set union of left and right child result sets
			String keyspace = query.getKeyspace();
			QueryElement rootElement = query.getRootElement();
			return this.evaluateRecursive(rootElement, timestamp, branch, keyspace).iterator();
		});
	}

	@Override
	public long evaluateCount(final long timestamp, final Branch branch, final ChronoDBQuery query) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must be >= 0!");
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		return this.owningDB.performNonExclusive(() -> {
			// TODO PERFORMANCE: evaluating everything and then counting is not very efficient...
			String keyspace = query.getKeyspace();
			QueryElement rootElement = query.getRootElement();
			Set<QualifiedKey> resultSet = this.evaluateRecursive(rootElement, timestamp, branch, keyspace);
			return resultSet.size();
		});
	}

	// =====================================================================================================================
	// ROLLBACK METHODS
	// =====================================================================================================================

	@Override
	public void rollback(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		this.owningDB.performExclusive(() -> {
			Set<String> branchNames = this.owningDB.getBranchManager().getBranchNames();
			this.backend.rollback(branchNames, timestamp);
			this.clearQueryCache();
		});
	}

	@Override
	public void rollback(final Branch branch, final long timestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		this.owningDB.performExclusive(() -> {
			this.backend.rollback(Collections.singleton(branch.getName()), timestamp);
			this.clearQueryCache();
		});
	}

	@Override
	public void rollback(final Branch branch, final long timestamp, final Set<QualifiedKey> keys) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(keys, "Precondition violation - argument 'keys' must not be NULL!");
		this.owningDB.performExclusive(() -> {
			this.backend.rollback(Collections.singleton(branch.getName()), timestamp, keys);
		});
	}

	@Override
	public void clearQueryCache() {
		if (this.queryCache != null) {
			this.queryCache.clear();
		}
	}

	@VisibleForTesting
	public ChronoIndexQueryCache getIndexQueryCache() {
		return this.queryCache;
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private void setIndexDirty(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		Boolean previous = this.indexNameToDirtyFlag.put(indexName, true);
		if (previous == null || previous == false) {
			this.backend.persistIndexDirtyStates(this.indexNameToDirtyFlag);
		}
	}

	private void setIndexClean(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		Boolean previous = this.indexNameToDirtyFlag.put(indexName, false);
		if (previous == null || previous == true) {
			this.backend.persistIndexDirtyStates(this.indexNameToDirtyFlag);
		}
	}

	private Set<ChronoIdentifier> performIndexQuery(final long timestamp, final Branch branch,
			final SearchSpecification searchSpec) {
		return this.owningDB.performNonExclusive(() -> {
			Collection<ChronoIndexDocument> documents = this.backend.getMatchingDocuments(timestamp, branch,
					searchSpec);
			Set<ChronoIdentifier> resultSet = Sets.newHashSet();
			for (ChronoIndexDocument document : documents) {
				String keyspace = document.getKeyspace();
				String key = document.getKey();
				long docTimestamp = document.getValidFromTimestamp();
				// note: we explicitly use the branch passed as an argument to this method for the ChronoIdentifier
				// instead of the branch specified in the document. The reason is related to usabiltiy: the user
				// would likely expect to get back identifiers on the requested branch only. Behind the scenes,
				// the ACTUAL branch that really holds the value for the identifier may be any of the (direct
				// or transitive) origins of the given branch. It makes no difference for any get(...) operations,
				// because they traverse the branch origin hierarchy anyways, and it is nicer for the user if
				// the results match the requested branch, even when in truth the key-value pairs are stored in
				// one of the origin branches.
				ChronoIdentifier identifier = ChronoIdentifier.create(branch, docTimestamp, keyspace, key);
				resultSet.add(identifier);
			}
			return Collections.unmodifiableSet(resultSet);
		});
	}

	private Set<QualifiedKey> evaluateRecursive(final QueryElement element, final long timestamp, final Branch branch,
			final String keyspace) {
		Set<QualifiedKey> resultSet = Sets.newHashSet();
		if (element instanceof BinaryOperatorElement) {
			BinaryOperatorElement binaryOpElement = (BinaryOperatorElement) element;
			// disassemble the element
			QueryElement left = binaryOpElement.getLeftChild();
			QueryElement right = binaryOpElement.getRightChild();
			BinaryQueryOperator op = binaryOpElement.getOperator();
			// recursively evaluate left and right child result sets
			Set<QualifiedKey> leftResult = this.evaluateRecursive(left, timestamp, branch, keyspace);
			Set<QualifiedKey> rightResult = this.evaluateRecursive(right, timestamp, branch, keyspace);
			// depending on the operator, perform union or intersection
			switch (op) {
			case AND:
				// FIXME: we must not discard entries from different timestamps!
				resultSet.addAll(leftResult);
				resultSet.retainAll(rightResult);
				break;
			case OR:
				resultSet.addAll(leftResult);
				resultSet.addAll(rightResult);
				break;
			default:
				throw new UnknownEnumLiteralException(
						"Encountered unknown literal of BinaryQueryOperator: '" + op + "'!");
			}
			return Collections.unmodifiableSet(resultSet);
		} else if (element instanceof WhereElement) {
			WhereElement whereElement = (WhereElement) element;
			// disassemble and execute the atomic query
			String indexName = whereElement.getIndexName();
			Condition condition = whereElement.getCondition();
			TextMatchMode matchMode = whereElement.getMatchMode();
			String comparisonValue = whereElement.getComparisonValue();
			SearchSpecification searchSpec = SearchSpecification.create(indexName, condition, matchMode,
					comparisonValue);
			Set<ChronoIdentifier> identifiers = this.queryIndex(timestamp, branch, searchSpec);
			// remove the non-matching keyspaces and reduce from ChronoIdentifier to qualified key
			Set<QualifiedKey> filtered = identifiers.parallelStream()
					// remove non-matching keyspaces
					.filter(id -> id.getKeyspace().equals(keyspace))
					// we don't need timestamps, so convert into qualified keys instead
					.map(id -> QualifiedKey.create(id.getKeyspace(), id.getKey()))
					// ... and collect everything in a set
					.collect(Collectors.toSet());
			return Collections.unmodifiableSet(filtered);
		} else {
			// all other elements should be eliminated by optimizations...
			throw new ChronoDBQuerySyntaxException("Query contains unsupported element of class '"
					+ element.getClass().getName() + "' - was the query optimized?");
		}
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private class IndexingProcess {

		private long currentTimestamp = -1L;
		private ChronoIndexModifications indexModifications;
		private Branch branch;

		public void index(final Map<ChronoIdentifier, Pair<Object, Object>> identifierToValue) {
			checkNotNull(identifierToValue, "Precondition violation - argument 'identifierToValue' must not be NULL!");
			// build the indexer workload. The primary purpose is to sort the entries of the map in an order suitable
			// for processing.
			List<Pair<ChronoIdentifier, Pair<Object, Object>>> workload = IndexerWorkloadSorter.sort(identifierToValue);
			// get the iterator over the workload
			Iterator<Pair<ChronoIdentifier, Pair<Object, Object>>> iterator = workload.iterator();
			// iterate over the workload
			while (iterator.hasNext()) {
				Pair<ChronoIdentifier, Pair<Object, Object>> entry = iterator.next();
				// unwrap the chrono identifier and the value to index associated with it
				ChronoIdentifier chronoIdentifier = entry.getKey();
				// check if we need to perform any periodic tasks
				this.checkCurrentTimestamp(chronoIdentifier.getTimestamp());
				this.checkCurrentBranch(chronoIdentifier.getBranchName());
				// index the single entry
				Pair<Object, Object> oldAndNewValue = identifierToValue.get(chronoIdentifier);
				Object oldValue = oldAndNewValue.getLeft();
				Object newValue = oldAndNewValue.getRight();
				this.indexSingleEntry(chronoIdentifier, oldValue, newValue);
			}
			// apply any remaining index modifications
			if (this.indexModifications.isEmpty() == false) {
				StandardIndexManager.this.backend.applyModifications(this.indexModifications);
			}
		}

		private void checkCurrentBranch(final String nextBranchName) {
			if (this.branch == null || this.branch.getName().equals(nextBranchName) == false) {
				// the branch is not the same as in the previous entry. Fetch the
				// branch metadata from the database.
				this.branch = StandardIndexManager.this.owningDB.getBranchManager().getBranch(nextBranchName);
			}
		}

		private void checkCurrentTimestamp(final long nextTimestamp) {
			if (this.currentTimestamp < 0 || this.currentTimestamp != nextTimestamp) {
				// the timestamp of the new work item is different from the one before. We need
				// to apply any index modifications (if any) and open a new modifications object
				if (this.indexModifications != null) {
					StandardIndexManager.this.backend.applyModifications(this.indexModifications);
				}
				this.currentTimestamp = nextTimestamp;
				this.indexModifications = ChronoIndexModifications.create();
			}
		}

		private void indexSingleEntry(final ChronoIdentifier chronoIdentifier, final Object oldValue,
				final Object newValue) {
			// in order to correctly treat the deletions, we need to query the index backend for
			// the currently active documents. We load these on-demand, because we don't need them in
			// the common case of indexing previously unseen (new) elements.
			Map<String, Map<String, ChronoIndexDocument>> oldDocuments = null;
			// iterate over the known indices
			for (String indexName : StandardIndexManager.this.getIndexNames()) {
				// prepare the sets of values for that index
				Set<String> oldValues = this.getIndexedValuesForObject(indexName, oldValue);
				Set<String> newValues = this.getIndexedValuesForObject(indexName, newValue);
				// calculate the set differences
				Set<String> addedValues = Sets.newHashSet();
				addedValues.addAll(newValues);
				addedValues.removeAll(oldValues);
				Set<String> removedValues = Sets.newHashSet();
				removedValues.addAll(oldValues);
				removedValues.removeAll(newValues);
				// for each value we need to add, we create an index document based on the ChronoIdentifier.
				for (String addedValue : addedValues) {
					if (addedValue == null || addedValue.trim().isEmpty()) {
						// don't create documents for empty values
						continue;
					}
					this.indexModifications.addDocumentAddition(chronoIdentifier, indexName, addedValue);
				}
				// iterate over the removed values and terminate the document validities
				for (String removedValue : removedValues) {
					if (oldDocuments == null) {
						// make sure that the current index documents are available
						oldDocuments = StandardIndexManager.this.backend
								.getMatchingBranchLocalDocuments(chronoIdentifier);
					}
					Map<String, ChronoIndexDocument> indexedValueToOldDoc = oldDocuments.get(indexName);
					ChronoIndexDocument oldDocument = null;
					if (indexedValueToOldDoc != null) {
						oldDocument = indexedValueToOldDoc.get(removedValue);
					}
					if (oldDocument == null) {
						// There is no document for the old index value in our branch. This means that this indexed
						// value was never touched in our branch. To "simulate" a valdity termination, we
						// insert a new index document which is valid from the creation of our branch until
						// our current timestamp.
						ChronoIndexDocument document = new ChronoIndexDocumentImpl(indexName, this.branch.getName(),
								chronoIdentifier.getKeyspace(), chronoIdentifier.getKey(), removedValue,
								removedValue.toLowerCase(), this.branch.getBranchingTimestamp());
						document.setValidToTimestamp(this.currentTimestamp);
						this.indexModifications.addDocumentAddition(document);
					} else {
						// the document belongs to our branch; terminate its validity
						this.terminateDocumentValidityOrDeleteDocument(oldDocument, this.currentTimestamp);
					}
				}
			}
		}

		private Set<String> getIndexedValuesForObject(final String indexName, final Object object) {
			checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
			if (object == null) {
				// when the object to index is null, we can't produce any indexed values.
				// empty set is already unmodifiable, no need to wrap it in Collections.umodifiableSet.
				return Collections.emptySet();
			}
			Set<String> indexedValues = null;
			Set<ChronoIndexer> indexers = StandardIndexManager.this.indexNameToIndexers.get(indexName);
			for (ChronoIndexer indexer : indexers) {
				if (indexer.canIndex(object)) {
					if (indexedValues != null) {
						// we have a conflict - another indexer has already identified the new index value!
						throw new IndexerConflictException("There are multiple indexers on index '" + indexName
								+ "' capable of indexing an object of class '" + object.getClass().getName() + "'!");
					} else {
						indexedValues = indexer.getIndexValues(object);
						if (indexedValues == null) {
							// indexer returned a NULL set; replace it with empty set
							indexedValues = Collections.emptySet();
						} else {
							// make sure that there are no NULL values in the indexed values set
							indexedValues = indexedValues.stream().filter(e -> e != null).collect(Collectors.toSet());
						}
					}
				}
			}
			if (indexedValues == null) {
				// no indexer could index this element
				return Collections.emptySet();
			}
			return Collections.unmodifiableSet(indexedValues);
		}

		private void terminateDocumentValidityOrDeleteDocument(final ChronoIndexDocument document,
				final long timestamp) {
			checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
			checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
			// check when the document was created
			if (document.getValidFromTimestamp() >= timestamp) {
				// the document was created at the same timestamp where we are going
				// to terminate it. That makes no sense, because the time ranges are
				// inclusive in the lower bound and exclusive in the upper bound.
				// Therefore, if lowerbound == upper bound, then the document needs
				// to be deleted instead. This situation can appear during incremental
				// commits.
				this.indexModifications.addDocumentDeletion(document);
			} else {
				// regularly terminate the validity of this document
				this.indexModifications.addDocumentValidityTermination(document, timestamp);
			}
		}
	}
}
