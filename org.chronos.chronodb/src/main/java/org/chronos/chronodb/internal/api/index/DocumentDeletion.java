package org.chronos.chronodb.internal.api.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.impl.index.DocumentDeletionImpl;

public interface DocumentDeletion {

	public static DocumentDeletion create(final ChronoIndexDocument documentToDelete) {
		checkNotNull(documentToDelete, "Precondition violation - argument 'documentToDelete' must not be NULL!");
		return new DocumentDeletionImpl(documentToDelete);
	}

	public ChronoIndexDocument getDocumentToDelete();

}
