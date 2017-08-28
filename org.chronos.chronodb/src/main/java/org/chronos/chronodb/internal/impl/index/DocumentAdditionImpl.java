package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.DocumentAddition;

public class DocumentAdditionImpl implements DocumentAddition {

	private final ChronoIndexDocument document;

	public DocumentAdditionImpl(final ChronoIdentifier identifier, final String indexName, final Object indexValue) {
		this(new ChronoIndexDocumentImpl(identifier, indexName, indexValue));
	}

	public DocumentAdditionImpl(final ChronoIndexDocument document) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		this.document = document;
	}

	@Override
	public ChronoIndexDocument getDocumentToAdd() {
		return this.document;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ADD(");
		builder.append(this.document.getIndexName());
		builder.append("->");
		builder.append(this.document.getBranch());
		builder.append("->");
		builder.append(this.document.getKeyspace());
		builder.append("->");
		builder.append(this.document.getKey());
		builder.append(", value='");
		builder.append(this.document.getIndexedValue());
		builder.append("' (");
		builder.append(this.document.getIndexedValue().getClass().getName());
		builder.append("), ");
		builder.append(Period.createRange(this.document.getValidFromTimestamp(), this.document.getValidToTimestamp()));
		builder.append(")");
		return builder.toString();
	}

}
