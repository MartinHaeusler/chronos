package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

/**
 * A utility class that acts as a pure data container. It reflects the state of the secondary indexer in a given
 * keyspace.
 * 
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class IndexerKeyspaceState {

	// =================================================================================================================
	// BUILDER
	// =================================================================================================================

	public static IndexerKeyspaceState.Builder build(final String keyspace) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		return new Builder(keyspace);
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final String keyspace;

	private final Map<String, SetMultimap<String, ChronoIndexDocument>> indexNameToKeyToDocuments;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public IndexerKeyspaceState(final String keyspace) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		this.keyspace = keyspace;
		this.indexNameToKeyToDocuments = Maps.newHashMap();
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	public String getKeyspace() {
		return this.keyspace;
	}

	public Set<ChronoIndexDocument> getDocuments(final String indexName, final String key) {
		SetMultimap<String, ChronoIndexDocument> keyToDocs = this.indexNameToKeyToDocuments.get(indexName);
		Set<ChronoIndexDocument> indexDocuments = Collections.emptySet();
		if (keyToDocs != null) {
			indexDocuments = keyToDocs.get(key);
		}
		return indexDocuments;
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	private void addDocument(final ChronoIndexDocument document) {
		String indexName = document.getIndexName();
		SetMultimap<String, ChronoIndexDocument> keyToDocuments = this.indexNameToKeyToDocuments.get(indexName);
		if (keyToDocuments == null) {
			keyToDocuments = HashMultimap.create();
			this.indexNameToKeyToDocuments.put(indexName, keyToDocuments);
		}
		keyToDocuments.put(document.getKey(), document);
	}

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	public static class Builder {

		private final IndexerKeyspaceState state;

		public Builder(final String keyspace) {
			checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
			this.state = new IndexerKeyspaceState(keyspace);
		}

		public Builder addDocument(final ChronoIndexDocument document) {
			checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
			this.state.addDocument(document);
			return this;
		}

		public Builder addDocuments(final Iterable<? extends ChronoIndexDocument> documents) {
			checkNotNull(documents, "Precondition violation - argument 'documents' must not be NULL!");
			for (ChronoIndexDocument document : documents) {
				this.addDocument(document);
			}
			return this;
		}

		public IndexerKeyspaceState build() {
			return this.state;
		}
	}

}
