package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.ChronoIndexModifications;
import org.chronos.chronodb.internal.api.index.DocumentBasedIndexManagerBackend;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.index.diff.IndexValueDiff;
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils;
import org.chronos.common.autolock.AutoLock;

import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

public class DocumentBasedIndexManager
		extends AbstractBackendDelegatingIndexManager<ChronoDBInternal, DocumentBasedIndexManagerBackend> {

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public DocumentBasedIndexManager(final ChronoDBInternal owningDB, final DocumentBasedIndexManagerBackend backend) {
		super(owningDB, backend);
	}

	// =================================================================================================================
	// INDEXING METHODS
	// =================================================================================================================

	@Override
	public void reindex(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkArgument(this.getIndexNames().contains(indexName),
				"Precondition violation - argument 'indexName' does not refer to a known index!");
		this.reindexAll();
	}

	@Override
	public void reindexAll() {
		try (AutoLock lock = this.getOwningDB().lockExclusive()) {
			if (this.getDirtyIndices().isEmpty()) {
				// no indices are dirty -> no need to re-index
				return;
			}
			// first, delete whatever is in the index
			this.getIndexManagerBackend().deleteAllIndexContents();
			// then, iterate over the contents of the database
			BranchManager branchManager = this.getOwningDB().getBranchManager();
			Set<Branch> branches = branchManager.getBranches();
			Map<ChronoIdentifier, Pair<Object, Object>> identifierToValue = Maps.newHashMap();
			SerializationManager serializationManager = this.getOwningDB().getSerializationManager();
			// TODO PERFORMANCE: it's dangerous to simply load all entries; they might not fit into RAM!
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
			// clear the query cache
			this.clearQueryCache();
			for (String indexName : this.getIndexNames()) {
				this.setIndexClean(indexName);
			}
			this.getIndexManagerBackend().persistIndexDirtyStates(this.indexNameToDirtyFlag);
		}
	}

	@Override
	public void index(final Map<ChronoIdentifier, Pair<Object, Object>> identifierToOldAndNewValue) {
		checkNotNull(identifierToOldAndNewValue,
				"Precondition violation - argument 'identifierToOldAndNewValue' must not be NULL!");
		if (identifierToOldAndNewValue.isEmpty()) {
			// no workload to index
			return;
		}
		if (DocumentBasedIndexManager.this.getIndexNames().isEmpty()) {
			// no indices registered
			return;
		}
		try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
			new IndexingProcess().index(identifierToOldAndNewValue);
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	@Override
	protected Set<String> performIndexQuery(final long timestamp, final Branch branch, final String keyspace,
			final SearchSpecification<?> searchSpec) {
		try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
			// check if we are dealing with a negated search specification that accepts empty values.
			if (searchSpec.getCondition().isNegated() && searchSpec.getCondition().acceptsEmptyValue()) {
				// the search spec is a negated condition that accepts the empty value.
				// To resolve this condition:
				// - Call keySet() on the target keyspace
				// - query the index with the non-negated condition
				// - subtract the matches from the keyset
				Set<String> keySet = this.getOwningDB().tx(branch.getName(), timestamp).keySet(keyspace);
				SearchSpecification<?> nonNegatedSearch = searchSpec.negate();
				Collection<ChronoIndexDocument> documents = this.getIndexManagerBackend().getMatchingDocuments(timestamp,
						branch, keyspace, nonNegatedSearch);
				// subtract the matches from the keyset
				for (ChronoIndexDocument document : documents) {
					String key = document.getKey();
					keySet.remove(key);
				}
				return Collections.unmodifiableSet(keySet);
			} else {
				Collection<ChronoIndexDocument> documents = this.getIndexManagerBackend().getMatchingDocuments(timestamp,
						branch, keyspace, searchSpec);
				return Collections.unmodifiableSet(documents.stream().map(doc -> doc.getKey()).collect(Collectors.toSet()));
			}
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
				DocumentBasedIndexManager.this.getIndexManagerBackend().applyModifications(this.indexModifications);
			}
		}

		private void checkCurrentBranch(final String nextBranchName) {
			if (this.branch == null || this.branch.getName().equals(nextBranchName) == false) {
				// the branch is not the same as in the previous entry. Fetch the
				// branch metadata from the database.
				this.branch = DocumentBasedIndexManager.this.getOwningDB().getBranchManager().getBranch(nextBranchName);
			}
		}

		private void checkCurrentTimestamp(final long nextTimestamp) {
			if (this.currentTimestamp < 0 || this.currentTimestamp != nextTimestamp) {
				// the timestamp of the new work item is different from the one before. We need
				// to apply any index modifications (if any) and open a new modifications object
				if (this.indexModifications != null) {
					DocumentBasedIndexManager.this.getIndexManagerBackend().applyModifications(this.indexModifications);
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
			Map<String, SetMultimap<Object, ChronoIndexDocument>> oldDocuments = null;
			SetMultimap<String, Indexer<?>> indexNameToIndexers = DocumentBasedIndexManager.this.indexNameToIndexers;
			// calculate the diff
			IndexValueDiff diff = IndexingUtils.calculateDiff(indexNameToIndexers, oldValue, newValue);
			for (String indexName : diff.getChangedIndices()) {
				Set<Object> addedValues = diff.getAdditions(indexName);
				Set<Object> removedValues = diff.getRemovals(indexName);
				// for each value we need to add, we create an index document based on the ChronoIdentifier.
				for (Object addedValue : addedValues) {
					this.indexModifications.addDocumentAddition(chronoIdentifier, indexName, addedValue);
				}
				// iterate over the removed values and terminate the document validities
				for (Object removedValue : removedValues) {
					if (oldDocuments == null) {
						// make sure that the current index documents are available
						oldDocuments = DocumentBasedIndexManager.this.getIndexManagerBackend()
								.getMatchingBranchLocalDocuments(chronoIdentifier);
					}
					SetMultimap<Object, ChronoIndexDocument> indexedValueToOldDoc = oldDocuments.get(indexName);
					if (indexedValueToOldDoc == null) {
						// There is no document for the old index value in our branch. This means that this indexed
						// value was never touched in our branch. To "simulate" a valdity termination, we
						// insert a new index document which is valid from the creation of our branch until
						// our current timestamp.
						ChronoIndexDocument document = new ChronoIndexDocumentImpl(indexName, this.branch.getName(),
								chronoIdentifier.getKeyspace(), chronoIdentifier.getKey(), removedValue,
								this.branch.getBranchingTimestamp());
						document.setValidToTimestamp(this.currentTimestamp);
						this.indexModifications.addDocumentAddition(document);
					} else {
						Set<ChronoIndexDocument> oldDocs = indexedValueToOldDoc.get(removedValue);
						for (ChronoIndexDocument oldDocument : oldDocs) {
							if (oldDocument.getValidToTimestamp() < Long.MAX_VALUE) {
								// the document has already been closed. This can happen if a key-value pair has
								// been inserted into the store, later deleted, and later re-inserted.
								continue;
							} else {
								// the document belongs to our branch; terminate its validity
								this.terminateDocumentValidityOrDeleteDocument(oldDocument, this.currentTimestamp);
							}

						}
					}

				}
			}
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
