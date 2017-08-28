package org.chronos.chronodb.internal.impl.engines.inmemory;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.ChronoIndexModifications;
import org.chronos.chronodb.internal.api.index.DocumentAddition;
import org.chronos.chronodb.internal.api.index.DocumentDeletion;
import org.chronos.chronodb.internal.api.index.DocumentValidityTermination;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.impl.engines.base.AbstractDocumentBasedIndexManagerBackend;
import org.chronos.common.base.CCC;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.logging.LogLevel;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class InMemoryIndexManagerBackend extends AbstractDocumentBasedIndexManagerBackend {

	/** Index name -> Index Documents */
	protected final SetMultimap<String, ChronoIndexDocument> indexNameToDocuments;

	/** Index name -> Branch Name -> Keyspace Name -> Key -> Index Documents */
	protected final Map<String, Map<String, Map<String, SetMultimap<String, ChronoIndexDocument>>>> documents;

	/** Index name -> indexers */
	protected final SetMultimap<String, Indexer<?>> indexNameToIndexers;

	protected final Map<String, Boolean> indexNameToDirtyFlag;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public InMemoryIndexManagerBackend(final ChronoDB owningDB) {
		super(owningDB);
		this.indexNameToDocuments = HashMultimap.create();
		this.documents = Maps.newHashMap();
		this.indexNameToIndexers = HashMultimap.create();
		this.indexNameToDirtyFlag = Maps.newHashMap();
	}

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	@Override
	public SetMultimap<String, Indexer<?>> loadIndexersFromPersistence() {
		return HashMultimap.create(this.indexNameToIndexers);
	}

	@Override
	public void persistIndexers(final SetMultimap<String, Indexer<?>> indexNameToIndexers) {
		this.indexNameToIndexers.clear();
		this.indexNameToIndexers.putAll(indexNameToIndexers);
	}

	@Override
	public void deleteIndexAndIndexers(final String indexName) {
		this.indexNameToIndexers.removeAll(indexName);
		this.documents.remove(indexName);
		this.indexNameToDocuments.removeAll(indexName);
	}

	@Override
	public void deleteAllIndexContents() {
		this.documents.clear();
		this.indexNameToDocuments.clear();
	}

	@Override
	public void deleteAllIndicesAndIndexers() {
		this.indexNameToIndexers.clear();
		this.documents.clear();
		this.indexNameToDocuments.clear();
	}

	@Override
	public void deleteIndexContents(final String indexName) {
		this.documents.remove(indexName);
		this.indexNameToDocuments.removeAll(indexName);
	}

	@Override
	public void persistIndexer(final String indexName, final Indexer<?> indexer) {
		this.indexNameToIndexers.put(indexName, indexer);
	}

	// =================================================================================================================
	// INDEX DIRTY FLAG MANAGEMENT
	// =================================================================================================================

	@Override
	public Map<String, Boolean> loadIndexStates() {
		return Collections.unmodifiableMap(this.indexNameToDirtyFlag);
	}

	@Override
	public void persistIndexDirtyStates(final Map<String, Boolean> indexStates) {
		this.indexNameToDirtyFlag.clear();
		this.indexNameToDirtyFlag.putAll(indexStates);
	}

	// =================================================================================================================
	// INDEX DOCUMENT MANAGEMENT
	// =================================================================================================================

	@Override
	public void applyModifications(final ChronoIndexModifications indexModifications) {
		checkNotNull(indexModifications, "Precondition violation - argument 'indexModifications' must not be NULL!");
		if (indexModifications.isEmpty()) {
			return;
		}
		if (CCC.MIN_LOG_LEVEL.isLessThanOrEqualTo(LogLevel.TRACE)) {
			ChronoLogger.logTrace("Applying index modifications: " + indexModifications);
		}
		for (DocumentValidityTermination termination : indexModifications.getDocumentValidityTerminations()) {
			ChronoIndexDocument document = termination.getDocument();
			long timestamp = termination.getTerminationTimestamp();
			this.terminateDocumentValidity(document, timestamp);
		}
		for (DocumentAddition creation : indexModifications.getDocumentCreations()) {
			this.addDocument(creation.getDocumentToAdd());
		}
		for (DocumentDeletion deletion : indexModifications.getDocumentDeletions()) {
			ChronoIndexDocument document = deletion.getDocumentToDelete();
			String branchName = document.getBranch();
			String indexName = document.getIndexName();
			// remove from index-name-to-documents map
			this.indexNameToDocuments.remove(indexName, document);
			// remove from general documents map
			Map<String, Map<String, SetMultimap<String, ChronoIndexDocument>>> branchToKeyspaceToKey = this.documents
					.get(indexName);
			if (branchToKeyspaceToKey == null) {
				continue;
			}
			Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToKey = branchToKeyspaceToKey.get(branchName);
			if (keyspaceToKey == null) {
				continue;
			}
			SetMultimap<String, ChronoIndexDocument> keysToDocuments = keyspaceToKey.get(document.getKeyspace());
			if (keysToDocuments == null) {
				continue;
			}
			Set<ChronoIndexDocument> documents = keysToDocuments.get(document.getKey());
			if (documents == null) {
				continue;
			}
			documents.remove(document);
		}
	}

	@Override
	protected Set<ChronoIndexDocument> getDocumentsTouchedAtOrAfterTimestamp(final long timestamp,
			final Set<String> branches) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		Set<ChronoIndexDocument> resultSet = Sets.newHashSet();
		if (branches != null && branches.isEmpty()) {
			// no branches are requested, so the result set is empty by definition.
			return resultSet;
		}
		for (ChronoIndexDocument document : this.indexNameToDocuments.values()) {
			if (branches != null && branches.contains(document.getBranch()) == false) {
				// the branch of the document is not in the set of requested branches -> ignore the document
				continue;
			}
			if (document.getValidFromTimestamp() >= timestamp) {
				// the document was added at or after the timestamp in question
				resultSet.add(document);
			} else if (document.getValidToTimestamp() < Long.MAX_VALUE
					&& document.getValidToTimestamp() >= timestamp) {
				// the document was modified at or after the timestamp in question
				resultSet.add(document);
			}
		}
		return resultSet;
	}

	// =================================================================================================================
	// INDEX QUERYING
	// =================================================================================================================

	@Override
	public Map<String, SetMultimap<Object, ChronoIndexDocument>> getMatchingBranchLocalDocuments(
			final ChronoIdentifier chronoIdentifier) {
		checkNotNull(chronoIdentifier, "Precondition violation - argument 'chronoIdentifier' must not be NULL!");
		Map<String, SetMultimap<Object, ChronoIndexDocument>> indexToIndexedValueToDocument = Maps.newHashMap();
		for (Entry<String, Map<String, Map<String, SetMultimap<String, ChronoIndexDocument>>>> entry : this.documents
				.entrySet()) {
			String indexName = entry.getKey();
			Map<String, Map<String, SetMultimap<String, ChronoIndexDocument>>> branchToKeyspaceToKey = entry.getValue();
			Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToKey = branchToKeyspaceToKey
					.get(chronoIdentifier.getBranchName());
			if (keyspaceToKey == null) {
				continue;
			}
			SetMultimap<String, ChronoIndexDocument> keyToDocument = keyspaceToKey.get(chronoIdentifier.getKeyspace());
			if (keyToDocument == null) {
				continue;
			}
			Set<ChronoIndexDocument> documents = keyToDocument.get(chronoIdentifier.getKey());
			if (documents == null) {
				continue;
			}
			for (ChronoIndexDocument document : documents) {
				Object indexedValue = document.getIndexedValue();
				SetMultimap<Object, ChronoIndexDocument> indexedValueToDocuments = indexToIndexedValueToDocument.get(indexName);
				if (indexedValueToDocuments == null) {
					indexedValueToDocuments = HashMultimap.create();
					indexToIndexedValueToDocument.put(indexName, indexedValueToDocuments);
				}
				indexedValueToDocuments.put(indexedValue, document);
			}
		}
		return indexToIndexedValueToDocument;
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	@Override
	protected Set<ChronoIndexDocument> getMatchingBranchLocalDocuments(final long timestamp, final String branchName, final String keyspace,
			final SearchSpecification<?> searchSpec) {
		checkArgument(timestamp >= 0,
				"Precondition violation - argument 'timestamp' must be >= 0 (value: " + timestamp + ")!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		String indexName = searchSpec.getProperty();
		if (this.indexNameToIndexers.containsKey(indexName) == false) {
			throw new UnknownIndexException("There is no index named '" + indexName + "'!");
		}
		Map<String, Map<String, SetMultimap<String, ChronoIndexDocument>>> branchToKeyspace = this.documents
				.get(indexName);
		if (branchToKeyspace == null || branchToKeyspace.isEmpty()) {
			return Collections.emptySet();
		}
		Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToKeyToDoc = branchToKeyspace.get(branchName);
		if (keyspaceToKeyToDoc == null || keyspaceToKeyToDoc.isEmpty()) {
			return Collections.emptySet();
		}
		SetMultimap<String, ChronoIndexDocument> keyToDoc = keyspaceToKeyToDoc.get(keyspace);
		if (keyToDoc == null || keyToDoc.isEmpty()) {
			return Collections.emptySet();
		}
		Predicate<? super ChronoIndexDocument> filter = this.createMatchFilter(timestamp, searchSpec.toFilterPredicate());
		return Collections.unmodifiableSet(keyToDoc.values().parallelStream().filter(filter).collect(Collectors.toSet()));
	}

	@Override
	protected Set<ChronoIndexDocument> getTerminatedBranchLocalDocuments(final long timestamp, final String branchName, final String keyspace,
			final SearchSpecification<?> searchSpec) {
		checkArgument(timestamp >= 0,
				"Precondition violation - argument 'timestamp' must be >= 0 (value: " + timestamp + ")!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		String indexName = searchSpec.getProperty();
		if (this.indexNameToIndexers.containsKey(indexName) == false) {
			throw new UnknownIndexException("There is no index named '" + indexName + "'!");
		}
		Map<String, Map<String, SetMultimap<String, ChronoIndexDocument>>> branchToKeyspace = this.documents
				.get(indexName);
		if (branchToKeyspace == null || branchToKeyspace.isEmpty()) {
			return Collections.emptySet();
		}
		Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToKeyToDoc = branchToKeyspace.get(branchName);
		if (keyspaceToKeyToDoc == null || keyspaceToKeyToDoc.isEmpty()) {
			return Collections.emptySet();
		}
		SetMultimap<String, ChronoIndexDocument> keyToDoc = keyspaceToKeyToDoc.get(keyspace);
		if (keyToDoc == null || keyToDoc.isEmpty()) {
			return Collections.emptySet();
		}
		Predicate<? super ChronoIndexDocument> filter = this.createDeletionFilter(timestamp, searchSpec.toFilterPredicate());
		return Collections.unmodifiableSet(keyToDoc.values().parallelStream().filter(filter).collect(Collectors.toSet()));
	}

	private Predicate<? super ChronoIndexDocument> createMatchFilter(final long timestamp, final Predicate<Object> filterPredicate) {
		return (doc) -> {
			Object indexedValue = doc.getIndexedValue();
			boolean timeRangeOk = doc.getValidFromTimestamp() <= timestamp && timestamp < doc.getValidToTimestamp();
			if (timeRangeOk == false) {
				return false;
			}
			return filterPredicate.test(indexedValue);
		};
	}

	private Predicate<? super ChronoIndexDocument> createDeletionFilter(final long timestamp, final Predicate<Object> filterPredicate) {
		return (doc) -> {
			boolean timeRangeOk = doc.getValidToTimestamp() <= timestamp;
			if (timeRangeOk == false) {
				return false;
			}
			return filterPredicate.test(doc.getIndexedValue());
		};
	}

	protected void terminateDocumentValidity(final ChronoIndexDocument indexDocument, final long timestamp) {
		checkNotNull(indexDocument, "Precondition violation - argument 'indexDocument' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// for in-memory, we only need to set the termination timestamp
		indexDocument.setValidToTimestamp(timestamp);
	}

	protected void addDocument(final ChronoIndexDocument document) {
		String indexName = document.getIndexName();
		this.indexNameToDocuments.put(indexName, document);
		String branch = document.getBranch();
		String keyspace = document.getKeyspace();
		String key = document.getKey();
		Map<String, Map<String, SetMultimap<String, ChronoIndexDocument>>> branchToKeyspaceToKeyToDocs = this.documents
				.get(indexName);
		if (branchToKeyspaceToKeyToDocs == null) {
			branchToKeyspaceToKeyToDocs = Maps.newHashMap();
			this.documents.put(indexName, branchToKeyspaceToKeyToDocs);
		}
		Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToDocuments = branchToKeyspaceToKeyToDocs
				.get(branch);
		if (keyspaceToDocuments == null) {
			keyspaceToDocuments = Maps.newHashMap();
			branchToKeyspaceToKeyToDocs.put(branch, keyspaceToDocuments);
		}
		SetMultimap<String, ChronoIndexDocument> keyToDocuments = keyspaceToDocuments.get(keyspace);
		if (keyToDocuments == null) {
			keyToDocuments = HashMultimap.create();
			keyspaceToDocuments.put(keyspace, keyToDocuments);
		}
		keyToDocuments.put(key, document);
	}
}
