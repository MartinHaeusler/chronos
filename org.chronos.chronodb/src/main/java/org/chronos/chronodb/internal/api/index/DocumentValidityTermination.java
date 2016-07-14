package org.chronos.chronodb.internal.api.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.impl.index.DocumentValidityTerminationImpl;

public interface DocumentValidityTermination {

	public static DocumentValidityTermination create(final ChronoIndexDocument document,
			final long terminationTimestamp) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		checkArgument(terminationTimestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(terminationTimestamp > document.getValidFromTimestamp(),
				"Precondition violation - document validity cannot be terminated before/at the documents creation timestamp!");
		return new DocumentValidityTerminationImpl(document, terminationTimestamp);
	}

	public ChronoIndexDocument getDocument();

	public long getTerminationTimestamp();

}
