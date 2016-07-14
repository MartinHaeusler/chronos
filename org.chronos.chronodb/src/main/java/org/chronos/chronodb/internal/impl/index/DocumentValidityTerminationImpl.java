package org.chronos.chronodb.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.DocumentValidityTermination;

public class DocumentValidityTerminationImpl implements DocumentValidityTermination {

	private final ChronoIndexDocument document;
	private final long terminationTimestamp;

	public DocumentValidityTerminationImpl(final ChronoIndexDocument document, final long terminationTimestamp) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		checkArgument(terminationTimestamp >= 0, "Precondition violation - argument 'terminationTimestamp' must not be negative!");
		this.document = document;
		this.terminationTimestamp = terminationTimestamp;
	}

	@Override
	public ChronoIndexDocument getDocument() {
		return this.document;
	}

	@Override
	public long getTerminationTimestamp() {
		return this.terminationTimestamp;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TERMINATE(");
		builder.append(this.document.getIndexName());
		builder.append("->");
		builder.append(this.document.getKeyspace());
		builder.append("->");
		builder.append(this.document.getKey());
		builder.append("->");
		builder.append(this.document.getIndexedValue());
		builder.append("@");
		builder.append(this.terminationTimestamp);
		builder.append(")");
		return builder.toString();
	}

}
