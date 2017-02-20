package org.chronos.chronodb.internal.api.index;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.index.ChronoIndexModificationsImpl;

import com.google.common.collect.Maps;

public interface ChronoIndexModifications {

	// =================================================================================================================
	// FACTORY
	// =================================================================================================================

	public static ChronoIndexModifications create() {
		return new ChronoIndexModificationsImpl();
	}

	// =================================================================================================================
	// API
	// =================================================================================================================

	public void addDocumentValidityTermination(DocumentValidityTermination termination);

	public Set<DocumentValidityTermination> getDocumentValidityTerminations();

	public void addDocumentCreation(DocumentAddition creation);

	public Set<DocumentAddition> getDocumentCreations();

	public void addDocumentDeletion(DocumentDeletion deletion);

	public Set<DocumentDeletion> getDocumentDeletions();

	public boolean isEmpty();

	// =================================================================================================================
	// CONVENIENCE METHODS
	// =================================================================================================================

	public default void addDocumentValidityTermination(final ChronoIndexDocument document, final long timestamp) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		DocumentValidityTermination termination = DocumentValidityTermination.create(document, timestamp);
		this.addDocumentValidityTermination(termination);
	}

	public default void addDocumentValidityTermination(final Set<ChronoIndexDocument> documents, final long timestamp) {
		checkNotNull(documents, "Precondition violation - argument 'documents' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		for (ChronoIndexDocument document : documents) {
			this.addDocumentValidityTermination(document, timestamp);
		}
	}

	public default void addDocumentAddition(final ChronoIdentifier identifier, final String indexName,
			final String indexValue) {
		checkNotNull(identifier, "Precondition violation - argument 'identifier' must not be NULL!");
		checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
		checkNotNull(indexValue, "Precondition violation - argument 'indexValue' must not be NULL!");
		this.addDocumentCreation(DocumentAddition.create(identifier, indexName, indexValue));
	}

	public default void addDocumentAddition(final ChronoIndexDocument indexDocument) {
		checkNotNull(indexDocument, "Precondition violation - argument 'indexDocument' must not be NULL!");
		this.addDocumentCreation(DocumentAddition.create(indexDocument));
	}

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
