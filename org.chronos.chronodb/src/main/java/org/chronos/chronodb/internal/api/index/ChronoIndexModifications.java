package org.chronos.chronodb.internal.api.index;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.index.ChronoIndexModificationsImpl;

import com.google.common.collect.Maps;

/**
 * Instances of this class represent a collection of modifications that should be applied on the secondary index.
 *
 * <p>
 * Essentially, this class is a wrapper around a collection of modification objects. The benefit of this class is the easier API compared to plain collections, as well as additional utility methods (e.g. {@link #groupByBranch()}).
 *
 * <p>
 * You can create a new instance of this class via the static factory method {@link #create()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoIndexModifications {

	// =================================================================================================================
	// FACTORY
	// =================================================================================================================

	/**
	 * Creates a new, empty {@link ChronoIndexModifications} instance.
	 *
	 * @return The newly created instance. Never <code>null</code>.
	 */
	public static ChronoIndexModifications create() {
		return new ChronoIndexModificationsImpl();
	}

	// =================================================================================================================
	// API
	// =================================================================================================================

	/**
	 * Adds the given document validity termination to this collection of modifications.
	 *
	 * @param termination
	 *            The document termination to add. Must not be <code>null</code>.
	 */
	public void addDocumentValidityTermination(DocumentValidityTermination termination);

	/**
	 * Returns the document validity terminations contained in this collection of modifications.
	 *
	 * @return The document validity terminations. Never <code>null</code>, may be empty.
	 */
	public Set<DocumentValidityTermination> getDocumentValidityTerminations();

	/**
	 * Adds the given document creation to this collection of modifications.
	 *
	 * @param creation
	 *            The document creation to add. Must not be <code>null</code>.
	 */
	public void addDocumentCreation(DocumentAddition creation);

	/**
	 * Returns the set of document additions contained in this collection of modifications.
	 *
	 * @return The document additions. Never <code>null</code>, may be empty.
	 */
	public Set<DocumentAddition> getDocumentCreations();

	/**
	 * Adds the given document deletion to this collection of modifications.
	 *
	 * @param deletion
	 *            The document deletion to add. Must not be <code>null</code>.
	 */
	public void addDocumentDeletion(DocumentDeletion deletion);

	/**
	 * Returns the set of document deletions contained in this collection of modifications.
	 *
	 * @return The document deletions. Never <code>null</code>, may be empty.
	 */
	public Set<DocumentDeletion> getDocumentDeletions();

	/**
	 * Checks if this collection of modifications is empty or not.
	 *
	 * @return <code>true</code> if there are no modifications contained in this collection, otherwise <code>false</code>.
	 */
	public boolean isEmpty();

	// =================================================================================================================
	// CONVENIENCE METHODS
	// =================================================================================================================

	/**
	 * Adds the validity termination of the given document at the given timestamp.
	 *
	 * @param document
	 *            The document to terminate the validity range for. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp at which the validity should be terminated. Must not be negative.
	 */
	public default void addDocumentValidityTermination(final ChronoIndexDocument document, final long timestamp) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		DocumentValidityTermination termination = DocumentValidityTermination.create(document, timestamp);
		this.addDocumentValidityTermination(termination);
	}

	/**
	 * Adds one validity termination for each of the given documents, at the given timestamp.
	 *
	 * @param documents
	 *            The documents to terminate the validity for. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp at which to terminate the validity of the given documents. Must not be negative.
	 */
	public default void addDocumentValidityTermination(final Set<ChronoIndexDocument> documents, final long timestamp) {
		checkNotNull(documents, "Precondition violation - argument 'documents' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		for (ChronoIndexDocument document : documents) {
			this.addDocumentValidityTermination(document, timestamp);
		}
	}

	/**
	 * Adds a new document addition with the given parameters.
	 *
	 * @param identifier
	 *            The {@link ChronoIdentifier} to which the new document should refer. Must not be <code>null</code>.
	 * @param indexName
	 *            The name of the index to which the new document belongs. Must not be <code>null</code>.
	 * @param indexValue
	 *            The indexed value to store in the new document. Must not be <code>null</code>.
	 */
	public default void addDocumentAddition(final ChronoIdentifier identifier, final String indexName,
			final Object indexValue) {
		checkNotNull(identifier, "Precondition violation - argument 'identifier' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(indexValue, "Precondition violation - argument 'indexValue' must not be NULL!");
		this.addDocumentCreation(DocumentAddition.create(identifier, indexName, indexValue));
	}

	/**
	 * Adds a new {@linkplain DocumentAddition document addition} for the given index document.
	 *
	 * @param indexDocument
	 *            The document to create the document addition for. Must not be <code>null</code>.
	 */
	public default void addDocumentAddition(final ChronoIndexDocument indexDocument) {
		checkNotNull(indexDocument, "Precondition violation - argument 'indexDocument' must not be NULL!");
		this.addDocumentCreation(DocumentAddition.create(indexDocument));
	}

	/**
	 * Adds a new {@linkplain DocumentDeletion document deletion} for the given document.
	 *
	 * @param document
	 *            The document to add the deletion for. Must not be <code>null</code>.
	 */
	public default void addDocumentDeletion(final ChronoIndexDocument document) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		this.addDocumentDeletion(DocumentDeletion.create(document));
	}

	/**
	 * Splits the modifications currently stored in this object into new modifications, grouped by branch.
	 *
	 * <p>
	 * This will <b>not</b> change the current state of this object. Any further modifications to this object will <b>not</b> be reflected in the resulting groups.
	 *
	 * <p>
	 * If a branch has no modifications in this object, there will be <b>no entry</b> for that branch in the resulting map.
	 *
	 * @return The mapping from branch name to modifications. Never <code>null</code>, may be empty.
	 */
	public default Map<String, ChronoIndexModifications> groupByBranch() {
		Map<String, ChronoIndexModifications> branchToModifications = Maps.newHashMap();
		for (DocumentAddition addition : this.getDocumentCreations()) {
			String branch = addition.getDocumentToAdd().getBranch();
			ChronoIndexModifications branchModifications = branchToModifications.get(branch);
			if (branchModifications == null) {
				branchModifications = ChronoIndexModifications.create();
				branchToModifications.put(branch, branchModifications);
			}
			branchModifications.addDocumentCreation(addition);
		}
		for (DocumentDeletion deletion : this.getDocumentDeletions()) {
			String branch = deletion.getDocumentToDelete().getBranch();
			ChronoIndexModifications branchModifications = branchToModifications.get(branch);
			if (branchModifications == null) {
				branchModifications = ChronoIndexModifications.create();
				branchToModifications.put(branch, branchModifications);
			}
			branchModifications.addDocumentDeletion(deletion);
		}
		for (DocumentValidityTermination validityTermination : this.getDocumentValidityTerminations()) {
			String branch = validityTermination.getDocument().getBranch();
			ChronoIndexModifications branchModifications = branchToModifications.get(branch);
			if (branchModifications == null) {
				branchModifications = ChronoIndexModifications.create();
				branchToModifications.put(branch, branchModifications);
			}
			branchModifications.addDocumentValidityTermination(validityTermination);
		}
		return branchToModifications;
	}

}
