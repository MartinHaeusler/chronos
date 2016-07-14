package org.chronos.chronodb.internal.impl.engines.mapdb;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.ChronoIndexModifications;
import org.chronos.chronodb.internal.api.index.DocumentAddition;
import org.chronos.chronodb.internal.api.index.DocumentDeletion;
import org.chronos.chronodb.internal.api.index.DocumentValidityTermination;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronodb.internal.impl.engines.base.AbstractIndexManagerBackend;
import org.mapdb.Atomic.Var;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class MapDBIndexManagerBackend extends AbstractIndexManagerBackend {

	private static final String VARIABLE_NAME_INDEXERS = "chronodb_indexers";
	private static final String VARIABLE_NAME_DIRTY_FLAGS = "chronodb_indexdirty";

	private static final String INDEX_DIRECTORY_PREFIX = "temporalIndex";

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private LuceneWrapper lucene;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public MapDBIndexManagerBackend(final MapDBChronoDB owningDB) {
		super(owningDB);
		this.lucene = new LuceneWrapper(this.getIndexDirectory());
		this.initializeShutdownHook();
	}

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	@Override
	public SetMultimap<String, ChronoIndexer> loadIndexersFromPersistence() {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			return this.loadIndexersMap(tx);
		}
	}

	@Override
	public void persistIndexers(final SetMultimap<String, ChronoIndexer> indexNameToIndexers) {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			this.persistIndexersMap(indexNameToIndexers, tx);
			tx.commit();
		}
	}

	@Override
	public void deleteIndexAndIndexers(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		// first, delete the indexers
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			SetMultimap<String, ChronoIndexer> indexersMap = this.loadIndexersFromPersistence();
			indexersMap.removeAll(indexName);
			this.persistIndexers(indexersMap);
			tx.commit();
		}
		// then, delete the lucene documents
		this.lucene.deleteDocumentsByIndexName(indexName);
	}

	@Override
	public void deleteAllIndicesAndIndexers() {
		// first, delete the indexers
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			SetMultimap<String, ChronoIndexer> indexersMap = HashMultimap.create();
			this.persistIndexers(indexersMap);
			tx.commit();
		}
		// then, shutdown the lucene instance
		this.lucene.close();
		// delete the directory contents
		File directory = this.getIndexDirectory();
		try {
			FileUtils.cleanDirectory(directory);
		} catch (IOException e) {
			throw new ChronoDBStorageBackendException("Could not delete indexing directory!", e);
		}
		// set up a new lucene instance
		this.lucene = new LuceneWrapper(directory);
	}

	@Override
	public void deleteIndexContents(final String indexName) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		this.lucene.deleteDocumentsByIndexName(indexName);
	}

	@Override
	public void persistIndexer(final String indexName, final ChronoIndexer indexer) {
		// TODO PERFORMANCE MAPDB: Storing the entire map just to add one indexer is not very efficient.
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			SetMultimap<String, ChronoIndexer> map = this.loadIndexersMap(tx);
			map.put(indexName, indexer);
			this.persistIndexersMap(map, tx);
			tx.commit();
		}
	}

	// =================================================================================================================
	// INDEX DIRTY FLAG MANAGEMENT
	// =================================================================================================================

	@Override
	public Map<String, Boolean> loadIndexStates() {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			byte[] dirtyFlagsSerialized = this.getIndexDirtyMapVariable(tx).get();
			Map<String, Boolean> map = this.deserializeObject(dirtyFlagsSerialized);
			if (map == null) {
				return Maps.newHashMap();
			} else {
				return map;
			}
		}
	}

	@Override
	public void persistIndexDirtyStates(final Map<String, Boolean> indices) {
		try (MapDBTransaction tx = this.getOwningDB().openTransaction()) {
			byte[] serializedForm = this.serializeObject(indices);
			this.getIndexDirtyMapVariable(tx).set(serializedForm);
			tx.commit();
		}
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
		this.lucene.performIndexWrite(indexWriter -> {
			for (DocumentValidityTermination termination : indexModifications.getDocumentValidityTerminations()) {
				ChronoIndexDocument document = termination.getDocument();
				long terminationTimestamp = termination.getTerminationTimestamp();
				this.terminateDocumentValidity(indexWriter, document, terminationTimestamp);
			}
			for (DocumentAddition creation : indexModifications.getDocumentCreations()) {
				ChronoIndexDocument documentToAdd = creation.getDocumentToAdd();
				this.persistDocument(indexWriter, documentToAdd);
			}
			List<Term> deleteDocumentTerms = Lists.newArrayList();
			for (DocumentDeletion deletion : indexModifications.getDocumentDeletions()) {
				String id = deletion.getDocumentToDelete().getDocumentId();
				deleteDocumentTerms.add(new Term(ChronoDBLuceneUtil.DOCUMENT_FIELD_ID, id));
			}
			if (deleteDocumentTerms.isEmpty() == false) {
				// perform the deletions
				indexWriter.deleteDocuments(deleteDocumentTerms.toArray(new Term[deleteDocumentTerms.size()]));
			}
		});
	}

	// @Override
	// public IndexerKeyspaceState getLatestIndexDocumentsFor(final String branch, final String keyspace) {
	// checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
	// List<Document> luceneDocuments = this.lucene.getLatestLuceneDocumentsForChronoIdentifier(keyspace);
	// IndexerKeyspaceState.Builder builder = IndexerKeyspaceState.build(keyspace);
	// if (luceneDocuments == null || luceneDocuments.isEmpty()) {
	// // no results; return the empty state
	// return builder.build();
	// }
	// // convert each lucene document into an index document and add it to the result map
	// for (Document luceneDocument : luceneDocuments) {
	// ChronoIndexDocument indexDocument = ChronoDBLuceneUtil
	// .convertLuceneDocumentToChronoDocument(luceneDocument);
	// builder.addDocument(indexDocument);
	// }
	// return builder.build();
	// }

	@Override
	protected Set<ChronoIndexDocument> getDocumentsTouchedAtOrAfterTimestamp(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		List<Document> luceneDocs = this.lucene.getDocumentsTouchedAtOrAfterTimestamp(timestamp);
		return ChronoDBLuceneUtil.convertLuceneDocumentsToChronoDocuments(luceneDocs);
	}

	// =================================================================================================================
	// INDEX QUERYING
	// =================================================================================================================

	@Override
	public Map<String, Map<String, ChronoIndexDocument>> getMatchingBranchLocalDocuments(final ChronoIdentifier chronoIdentifier) {
		checkNotNull(chronoIdentifier, "Precondition violation - argument 'chronoIdentifier' must not be NULL!");
		// run the lucene query
		List<Document> documents = this.lucene.getMatchingBranchLocalDocuments(chronoIdentifier);
		// sort the resulting documents into the required structure and convert lucene docs to chrono index docs
		Map<String, Map<String, ChronoIndexDocument>> resultMap = Maps.newHashMap();
		for (Document luceneDoc : documents) {
			ChronoIndexDocument chronoDoc = ChronoDBLuceneUtil.convertLuceneDocumentToChronoDocument(luceneDoc);
			String indexName = chronoDoc.getIndexName();
			Map<String, ChronoIndexDocument> indexValueToDocument = resultMap.get(indexName);
			if (indexValueToDocument == null) {
				indexValueToDocument = Maps.newHashMap();
				resultMap.put(indexName, indexValueToDocument);
			}
			indexValueToDocument.put(chronoDoc.getIndexedValue(), chronoDoc);
		}
		return resultMap;
	}

	// =================================================================================================================
	// INTERNAL UTILITY METHODS
	// =================================================================================================================

	@Override
	protected Collection<ChronoIndexDocument> getTerminatedBranchLocalDocuments(final long timestamp, final String branchName, final SearchSpecification searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		Collection<Document> luceneDocs = this.lucene.getTerminatedBranchLocalDocuments(timestamp, branchName, searchSpec);
		// transform into chrono index documents
		return ChronoDBLuceneUtil.convertLuceneDocumentsToChronoDocuments(luceneDocs);
	}

	@Override
	protected Collection<ChronoIndexDocument> getMatchingBranchLocalDocuments(final long timestamp, final String branchName, final SearchSpecification searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		String indexName = searchSpec.getProperty();
		// check if the index exists on the branch
		Boolean indexState = this.loadIndexStates().get(indexName);
		if (indexState == null) {
			// index does not exist!
			throw new UnknownIndexException("There is no index named '" + indexName + "'!");
		}
		List<Document> luceneDocuments = this.lucene.getMatchingBranchLocalDocuments(timestamp, branchName, searchSpec);
		return ChronoDBLuceneUtil.convertLuceneDocumentsToChronoDocuments(luceneDocuments);
	}

	private String getIndexDirectoryName() {
		// NOTE: this name for the index directory was kept for compatibility reasons. It originates from
		// the time when each branch had its own indexer.
		return INDEX_DIRECTORY_PREFIX + "_" + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
	}

	private File getIndexDirectory() {
		return new File(this.getOwningDB().getDirectory(), this.getIndexDirectoryName());
	}

	private Var<byte[]> getIndexersMapVariable(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.atomicVar(VARIABLE_NAME_INDEXERS + "_" + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	private Var<byte[]> getIndexDirtyMapVariable(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return tx.atomicVar(VARIABLE_NAME_DIRTY_FLAGS + "_" + ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	private <T> byte[] serializeObject(final T object) {
		return this.owningDB.getSerializationManager().serialize(object);
	}

	@SuppressWarnings("unchecked")
	private <T> T deserializeObject(final byte[] serializedForm) {
		if (serializedForm == null || serializedForm.length <= 0) {
			return null;
		}
		return (T) this.owningDB.getSerializationManager().deserialize(serializedForm);
	}

	private SetMultimap<String, ChronoIndexer> loadIndexersMap(final MapDBTransaction tx) {
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		Var<byte[]> variable = this.getIndexersMapVariable(tx);
		byte[] serializedForm = variable.get();
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

	private void persistIndexersMap(final SetMultimap<String, ChronoIndexer> indexNameToIndexers, final MapDBTransaction tx) {
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
		Var<byte[]> variable = this.getIndexersMapVariable(tx);
		variable.set(serialForm);
	}

	private void persistDocument(final IndexWriter indexWriter, final ChronoIndexDocument document) throws IOException {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		Document luceneDocument = ChronoDBLuceneUtil.convertChronoDocumentToLuceneDocument(document);
		indexWriter.addDocument(luceneDocument);
	}

	private void terminateDocumentValidity(final IndexWriter indexWriter, final ChronoIndexDocument indexDocument, final long timestamp) throws IOException {
		checkNotNull(indexWriter, "Precondition violation - argument 'indexWriter' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// remember the "valid to" timestamp before we execute the operation to reset it if the operation fails
		long validToBefore = indexDocument.getValidToTimestamp();
		try {
			String id = indexDocument.getDocumentId();
			indexDocument.setValidToTimestamp(timestamp);
			Document luceneDocument = ChronoDBLuceneUtil.convertChronoDocumentToLuceneDocument(indexDocument);
			indexWriter.updateDocument(new Term(ChronoDBLuceneUtil.DOCUMENT_FIELD_ID, id), luceneDocument);
		} catch (IOException e) {
			// operation failed, set the "valid to" timestamp back to what it was before
			indexDocument.setValidToTimestamp(validToBefore);
			throw e;
		}
	}

	protected MapDBChronoDB getOwningDB() {
		return (MapDBChronoDB) this.owningDB;
	}

	// =================================================================================================================
	// SHUTDOWN
	// =================================================================================================================

	private void initializeShutdownHook() {
		this.getOwningDB().addShutdownHook(() -> {
			if (this.lucene.isClosed() == false) {
				this.lucene.close();
			}
		});
	}
}
