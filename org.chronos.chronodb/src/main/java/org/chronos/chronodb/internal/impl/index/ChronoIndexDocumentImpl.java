package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.UUID;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;

public class ChronoIndexDocumentImpl implements ChronoIndexDocument {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final String documentId;
	private final String branch;
	private final String indexName;
	private final String keyspace;
	private final String key;
	private final String indexedValue;
	private final String indexedValueCaseInsensitive;
	private final long validFrom;
	private long validTo;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChronoIndexDocumentImpl(final ChronoIdentifier identifier, final String indexName, final String indexValue,
			final String indexedValueCaseInsensitive) {
		this(indexName, identifier.getBranchName(), identifier.getKeyspace(), identifier.getKey(), indexValue,
				indexedValueCaseInsensitive, identifier.getTimestamp());
	}

	public ChronoIndexDocumentImpl(final String indexName, final String branchName, final String keyspace,
			final String key, final String indexedValue, final String indexedValueCaseInsensitive,
			final long validFrom) {
		this(UUID.randomUUID().toString(), indexName, branchName, keyspace, key, indexedValue,
				indexedValueCaseInsensitive, validFrom, Long.MAX_VALUE);
	}

	public ChronoIndexDocumentImpl(final String id, final String indexName, final String branchName,
			final String keyspace, final String key, final String indexedValue,
			final String indexedValueCaseInsensitive, final long validFrom, final long validTo) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(indexedValue, "Precondition violation - argument 'indexedValue' must not be NULL!");
		checkNotNull(indexedValueCaseInsensitive,
				"Precondition violation - argument 'indexedValueCaseInsensitive' must not be NULL!");
		checkArgument(validFrom >= 0,
				"Precondition violation - argument 'validFrom' must be >= 0 (value: " + validFrom + ")!");
		checkArgument(validTo >= 0,
				"Precondition violation - argument 'validTo' must be >= 0 (value: " + validTo + ")!");
		checkArgument(validFrom < validTo, "Precondition violation - argument 'validTo' must be > 'validFrom'!");
		this.documentId = id;
		this.indexName = indexName;
		this.branch = branchName;
		this.keyspace = keyspace;
		this.key = key;
		this.indexedValue = indexedValue;
		this.indexedValueCaseInsensitive = indexedValueCaseInsensitive;
		this.validFrom = validFrom;
		this.validTo = validTo;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public String getDocumentId() {
		return this.documentId;
	}

	@Override
	public String getIndexName() {
		return this.indexName;
	}

	@Override
	public String getBranch() {
		return this.branch;
	}

	@Override
	public String getKeyspace() {
		return this.keyspace;
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String getIndexedValue() {
		return this.indexedValue;
	}

	@Override
	public String getIndexedValueCaseInsensitive() {
		return this.indexedValueCaseInsensitive;
	}

	@Override
	public long getValidFromTimestamp() {
		return this.validFrom;
	}

	@Override
	public long getValidToTimestamp() {
		return this.validTo;
	}

	@Override
	public void setValidToTimestamp(final long validTo) {
		checkArgument(validTo > this.validFrom, "Precondition violation - argument 'validTo' must be > 'validFrom'!");
		this.validTo = validTo;
	}

	// =================================================================================================================
	// HASH CODE & EQUALS
	// =================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.documentId == null ? 0 : this.documentId.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		ChronoIndexDocumentImpl other = (ChronoIndexDocumentImpl) obj;
		if (this.documentId == null) {
			if (other.documentId != null) {
				return false;
			}
		} else if (!this.documentId.equals(other.documentId)) {
			return false;
		}
		return true;
	}

	// =================================================================================================================
	// TO STRING
	// =================================================================================================================

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("IndexDoc['");
		builder.append(this.getIndexName());
		builder.append("'->'");
		builder.append(this.getKeyspace());
		builder.append("'->'");
		builder.append(this.getKey());
		builder.append("' = '");
		builder.append(this.getIndexedValue());
		builder.append("', [");
		builder.append(this.getValidFromTimestamp());
		builder.append(";");
		builder.append(this.getValidToTimestamp());
		builder.append("]");
		return builder.toString();
	}

}
