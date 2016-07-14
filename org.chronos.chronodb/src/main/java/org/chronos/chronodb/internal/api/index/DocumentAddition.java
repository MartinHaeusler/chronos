package org.chronos.chronodb.internal.api.index;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.index.DocumentAdditionImpl;

public interface DocumentAddition {

	public static DocumentAddition create(final ChronoIdentifier identifier, final String indexName,
			final String indexValue) {
		return new DocumentAdditionImpl(identifier, indexName, indexValue);
	}

	public static DocumentAddition create(final ChronoIndexDocument documentToAdd) {
		return new DocumentAdditionImpl(documentToAdd);
	}

	public ChronoIndexDocument getDocumentToAdd();
}
