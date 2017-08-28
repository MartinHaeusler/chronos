package org.chronos.chronodb.internal.impl.engines.chunkdb.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.impl.engines.chunkdb.ChunkedChronoDB;
import org.chronos.chronodb.internal.impl.engines.inmemory.InMemoryIndexManagerBackend;
import org.chronos.chronodb.internal.impl.index.ChronoIndexDocumentImpl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

public class DocumentBasedChunkIndex extends InMemoryIndexManagerBackend {

	private final String branchName;
	private boolean persistent;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public DocumentBasedChunkIndex(final ChunkedChronoDB owningDB, final DocumentChunkIndexData chunkIndexData) {
		super(owningDB);
		checkNotNull(chunkIndexData, "Precondition violation - argument 'chunkIndexData' must not be NULL!");
		this.branchName = chunkIndexData.getBranchName();
		this.indexNameToIndexers.putAll(chunkIndexData.getIndexersByPropertyName());
		this.loadDocumentsFromChunkData(chunkIndexData);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	/**
	 * Converts this instance into the persistable {@link DocumentChunkIndexData} format.
	 *
	 * <p>
	 * The returned object will contain a copy of the data stored in this index. This object can be used as usual after invoking this method, it will have no effect on the returned object.
	 *
	 * @return The persistable version of this index. The contained data is a copy, sharing no mutable references with the "live" system. Never <code>null</code>.
	 */
	public DocumentChunkIndexData toChunkIndexData() {
		// create the container object
		DocumentChunkIndexData chunkIndexData = new DocumentChunkIndexData();
		chunkIndexData.setBranchName(this.branchName);
		// add the indexers
		chunkIndexData.setIndexers(this.indexNameToIndexers);
		// fetch the documents
		Collection<ChronoIndexDocument> indexDocuments = this.indexNameToDocuments.values();
		// transform the index documents to their persistent format
		List<ChunkDbIndexDocumentData> list = indexDocuments.parallelStream().map(doc -> documentToData(doc))
				.collect(Collectors.toList());
		// add the documents
		chunkIndexData.addIndexDocuments(list);
		return chunkIndexData;
	}

	public boolean isPersistent() {
		return this.persistent;
	}

	public void setPersistent(final boolean persistent) {
		this.persistent = persistent;
	}

	@Override
	public Set<ChronoIndexDocument> getDocumentsTouchedAtOrAfterTimestamp(final long timestamp,
			final Set<String> branches) {
		// this method is repeated here because the visibility is increased to 'public'.
		return super.getDocumentsTouchedAtOrAfterTimestamp(timestamp, branches);
	}

	@Override
	public Set<ChronoIndexDocument> getTerminatedBranchLocalDocuments(final long timestamp, final String branchName, final String keyspace,
			final SearchSpecification<?> searchSpec) {
		// this method is repeated here because the visibility is increased to 'public'.
		return super.getTerminatedBranchLocalDocuments(timestamp, branchName, keyspace, searchSpec);
	}

	@Override
	public Set<ChronoIndexDocument> getMatchingBranchLocalDocuments(final long timestamp, final String branchName, final String keyspace,
			final SearchSpecification<?> searchSpec) {
		// this method is repeated here because the visibility is increased to 'public'.
		return super.getMatchingBranchLocalDocuments(timestamp, branchName, keyspace, searchSpec);
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	/**
	 * Loads the given data into this index.
	 *
	 * <p>
	 * This method is <code>private</code> on purpose; it must be called only once, and only by the constructor.
	 *
	 * @param chunkIndexData
	 *            The data to load. Must not be <code>null</code>.
	 */
	private void loadDocumentsFromChunkData(final DocumentChunkIndexData chunkIndexData) {
		checkNotNull(chunkIndexData, "Precondition violation - argument 'chunkIndexData' must not be NULL!");
		// load the indexers
		this.persistIndexers(chunkIndexData.getIndexersByPropertyName());
		// load the index document data
		List<ChunkDbIndexDocumentData> documentData = chunkIndexData.getIndexDocuments();
		// transform it into real documents
		List<ChronoIndexDocument> documents = documentData.stream().map(data -> dataToDocument(data, this.branchName))
				.collect(Collectors.toList());
		for (ChronoIndexDocument document : documents) {
			this.addDocument(document);
		}
	}

	/**
	 * Transforms the given {@link ChunkDbIndexDocumentData} into a {@link ChronoIndexDocument}.
	 *
	 * <p>
	 * Note: the resulting {@link ChronoIndexDocument} will have assigned a new random {@link UUID} as identifier. In particular, this means that invoking this method twice with the same parameters will yield two documents that are not {@link Object#equals(Object) equal} to each other.
	 *
	 * @param data
	 *            The data to be contained in the index document. Must not be <code>null</code>.
	 *
	 * @param branchName
	 *            The branch name for the resulting document. Must not be <code>null</code>.
	 *
	 * @return The index document, with a new random id assigned. Never <code>null</code>.
	 */
	private static ChronoIndexDocument dataToDocument(final ChunkDbIndexDocumentData data, final String branchName) {
		if (data == null) {
			throw new NullPointerException("Precondition violation - argument 'data' must not be NULL!");
		}
		if (branchName == null) {
			throw new NullPointerException("Precondition violation - argument 'branchName' must not be NULL!");
		}
		String id = UUID.randomUUID().toString();
		String indexName = data.getIndexName();
		String keyspace = data.getKeyspace();
		String key = data.getKey();
		Object indexedValue = data.getIndexedValue();
		long validFrom = data.getValidFromTimestamp();
		long validTo = data.getValidToTimestamp();
		return new ChronoIndexDocumentImpl(id, indexName, branchName, keyspace, key, indexedValue,
				validFrom, validTo);
	}

	/**
	 * Transforms the given {@link ChronoIndexDocument} into the persistable {@link ChunkDbIndexDocumentData} format.
	 *
	 * @param document
	 *            The document to transform. Must not be <code>null</code>.
	 *
	 * @return The document data, in persistable form. Never <code>null</code>.
	 */
	private static ChunkDbIndexDocumentData documentToData(final ChronoIndexDocument document) {
		if (document == null) {
			throw new NullPointerException("Precondition violation - argument 'document' must not be NULL!");
		}
		String indexName = document.getIndexName();
		String keyspace = document.getKeyspace();
		String key = document.getKey();
		Object value = document.getIndexedValue();
		long validFrom = document.getValidFromTimestamp();
		long validTo = document.getValidToTimestamp();
		return new ChunkDbIndexDocumentData(indexName, keyspace, key, value, validFrom, validTo);
	}

	@Override
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
