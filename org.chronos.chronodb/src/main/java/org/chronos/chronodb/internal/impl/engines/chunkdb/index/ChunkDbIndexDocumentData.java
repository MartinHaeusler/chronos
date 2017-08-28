package org.chronos.chronodb.internal.impl.engines.chunkdb.index;

import static com.google.common.base.Preconditions.*;

public class ChunkDbIndexDocumentData {

	private String indexName;
	private String keyspace;
	private String key;
	private Object indexedValue;
	private long validFrom;
	private long validTo;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected ChunkDbIndexDocumentData() {
		// default constructor for serialization
	}

	public ChunkDbIndexDocumentData(final String indexName, final String keyspace, final String key, final Object value,
			final long validFrom) {
		this(indexName, keyspace, key, value, validFrom, Long.MAX_VALUE);
	}

	public ChunkDbIndexDocumentData(final String indexName, final String keyspace, final String key, final Object value,
			final long validFrom, final long validTo) {
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		checkArgument(validFrom >= 0, "Precondition violation - argument 'validTo' must not be negative!");
		checkArgument(validFrom < validTo,
				"Precondition violation - argument 'validTo' must be larger than 'validFrom'!");
		this.indexName = indexName;
		this.keyspace = keyspace;
		this.key = key;
		this.indexedValue = value;
		this.validFrom = validFrom;
		this.validTo = validTo;
	}

	public String getIndexName() {
		return this.indexName;
	}

	public String getKeyspace() {
		return this.keyspace;
	}

	public String getKey() {
		return this.key;
	}

	public Object getIndexedValue() {
		return this.indexedValue;
	}

	public long getValidFromTimestamp() {
		return this.validFrom;
	}

	public long getValidToTimestamp() {
		return this.validTo;
	}

	public void setValidToTimestamp(final long validTo) {
		checkArgument(validTo > this.validFrom, "Precondition violation - argument 'validTo' must be > 'validFrom'!");
		this.validTo = validTo;
	}

}
