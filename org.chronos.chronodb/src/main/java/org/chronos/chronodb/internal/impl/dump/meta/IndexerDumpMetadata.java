package org.chronos.chronodb.internal.impl.dump.meta;

import java.util.Base64;

import org.chronos.chronodb.api.ChronoIndexer;
import org.chronos.common.serialization.KryoManager;

public class IndexerDumpMetadata {

	private String indexName;
	private String indexerData;

	public IndexerDumpMetadata() {
		// serialization constructor
	}

	public IndexerDumpMetadata(final String indexName, final ChronoIndexer indexer) {
		this.setIndexName(indexName);
		this.setIndexer(indexer);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public void setIndexName(final String indexName) {
		this.indexName = indexName;
	}

	public String getIndexName() {
		return this.indexName;
	}

	public void setIndexer(final ChronoIndexer indexer) {
		if (indexer == null) {
			this.indexerData = null;
		} else {
			byte[] serialForm = KryoManager.serialize(indexer);
			this.indexerData = Base64.getEncoder().encodeToString(serialForm);
		}
	}

	public ChronoIndexer getIndexer() {
		if (this.indexerData == null) {
			return null;
		} else {
			byte[] serialForm = Base64.getDecoder().decode(this.indexerData);
			return KryoManager.deserialize(serialForm);
		}
	}

}
