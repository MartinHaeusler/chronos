package org.chronos.chronodb.internal.api.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.impl.index.DocumentDeletionImpl;

/**
 * Change operation that deletes a {@link ChronoIndexDocument} from the secondary index.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface DocumentDeletion {

	/**
	 * Creates a new {@link DocumentDeletion} for the given document.
	 *
	 * @param documentToDelete
	 *            The document to delete from the secondary index. Must not be <code>null</code>.
	 * 
	 * @return The newly created document deletion. Never <code>null</code>.
	 */
	public static DocumentDeletion create(final ChronoIndexDocument documentToDelete) {
		checkNotNull(documentToDelete, "Precondition violation - argument 'documentToDelete' must not be NULL!");
		return new DocumentDeletionImpl(documentToDelete);
	}

	/**
	 * Returns the document to delete when executing this operation.
	 *
	 * @return The document to delete. Never <code>null</code>.
	 */
	public ChronoIndexDocument getDocumentToDelete();

}
