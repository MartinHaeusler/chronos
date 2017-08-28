package org.chronos.chronodb.internal.api.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.index.DocumentAdditionImpl;

/**
 * Change operation that adds a {@link ChronoIndexDocument} to the secondary index.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface DocumentAddition {

	/**
	 * Creates a new {@link DocumentAddition} instance with the given parameters.
	 *
	 * <p>
	 * When the addition created by this method is being executed, a new {@link ChronoIndexDocument} with the given parameters will be created on the fly.
	 *
	 * @param identifier
	 *            The {@link ChronoIdentifier} to which the new document should refer. Must not be <code>null</code>.
	 * @param indexName
	 *            The name of the index to which the new document should belong. Must not be <code>null</code>.
	 * @param indexValue
	 *            The indexed value to store in the new document. Must not be <code>null</code>.
	 *
	 * @return The newly created document addition. Never <code>null</code>.
	 */
	public static DocumentAddition create(final ChronoIdentifier identifier, final String indexName,
			final Object indexValue) {
		checkNotNull(identifier, "Precondition violation - argument 'identifier' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(indexValue, "Precondition violation - argument 'indexValue' must not be NULL!");
		return new DocumentAdditionImpl(identifier, indexName, indexValue);
	}

	/**
	 * Creates a new {@link DocumentAddition} for the given document.
	 *
	 * @param documentToAdd
	 *            The document to store in the addition. Must not be <code>null</code>.
	 *
	 * @return The newly created document addition. Never <code>null</code>.
	 */
	public static DocumentAddition create(final ChronoIndexDocument documentToAdd) {
		checkNotNull(documentToAdd, "Precondition violation - argument 'documentToAdd' must not be NULL!");
		return new DocumentAdditionImpl(documentToAdd);
	}

	/**
	 * Returns the document that should be added.
	 *
	 * @return The document to add when executing this operation. Never <code>null</code>.
	 */
	public ChronoIndexDocument getDocumentToAdd();
}
