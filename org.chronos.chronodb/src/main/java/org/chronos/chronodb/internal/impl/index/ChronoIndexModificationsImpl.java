package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;

import org.chronos.chronodb.internal.api.index.ChronoIndexModifications;
import org.chronos.chronodb.internal.api.index.DocumentAddition;
import org.chronos.chronodb.internal.api.index.DocumentDeletion;
import org.chronos.chronodb.internal.api.index.DocumentValidityTermination;

import com.google.common.collect.Sets;

public class ChronoIndexModificationsImpl implements ChronoIndexModifications {

	private final Set<DocumentValidityTermination> documentValidityTerminations;
	private final Set<DocumentAddition> documentCreations;
	private final Set<DocumentDeletion> documentDeletions;

	public ChronoIndexModificationsImpl() {
		this.documentValidityTerminations = Sets.newHashSet();
		this.documentCreations = Sets.newHashSet();
		this.documentDeletions = Sets.newHashSet();
	}

	@Override
	public void addDocumentValidityTermination(final DocumentValidityTermination termination) {
		checkNotNull(termination, "Precondition violation - argument 'termination' must not be NULL!");
		this.documentValidityTerminations.add(termination);
	}

	@Override
	public Set<DocumentValidityTermination> getDocumentValidityTerminations() {
		return Collections.unmodifiableSet(this.documentValidityTerminations);
	}

	@Override
	public void addDocumentCreation(final DocumentAddition creation) {
		checkNotNull(creation, "Precondition violation - argument 'creation' must not be NULL!");
		this.documentCreations.add(creation);
	}

	@Override
	public Set<DocumentAddition> getDocumentCreations() {
		return Collections.unmodifiableSet(this.documentCreations);
	}

	@Override
	public void addDocumentDeletion(final DocumentDeletion deletion) {
		checkNotNull(deletion, "Precondition violation - argument 'deletion' must not be NULL!");
		this.documentDeletions.add(deletion);
	}

	@Override
	public Set<DocumentDeletion> getDocumentDeletions() {
		return this.documentDeletions;
	}

	@Override
	public boolean isEmpty() {
		return this.documentCreations.isEmpty() && this.documentValidityTerminations.isEmpty() && this.documentDeletions.isEmpty();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("IndexModifications[");
		String separator = "";
		for (DocumentAddition creation : this.documentCreations) {
			builder.append(separator);
			separator = ", ";
			builder.append(creation.toString());
		}
		for (DocumentValidityTermination termination : this.documentValidityTerminations) {
			builder.append(separator);
			separator = ", ";
			builder.append(termination.toString());
		}
		for (DocumentDeletion deletion : this.documentDeletions) {
			builder.append(separator);
			separator = ", ";
			builder.append(deletion.toString());
		}
		builder.append("]");
		return builder.toString();
	}

}
