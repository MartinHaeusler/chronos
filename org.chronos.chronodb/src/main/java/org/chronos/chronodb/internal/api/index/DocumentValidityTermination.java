package org.chronos.chronodb.internal.api.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.impl.index.DocumentValidityTerminationImpl;

/**
 * Change operation that terminates the upper validity limit of a {@link ChronoIndexDocument}.
 *
 * <p>
 * Each index document has a time validity range, expressed as {@linkplain ChronoIndexDocument#getValidFromTimestamp() from} and {@linkplain ChronoIndexDocument#getValidToTimestamp() to} timestamps. This operation "terminates" the validity range by reducing the "to" value from infinity to a fixed value.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface DocumentValidityTermination {

	/**
	 * Creates a new {@link DocumentValidityTermination} for the given document, at the given timestamp.
	 *
	 * @param document
	 *            The document to terminate the validity range for. Must not be <code>null</code>.
	 * @param terminationTimestamp
	 *            The timestamp to terminate the upper validity range at. Must not be negative. Must be after the document's "from" value.
	 *
	 * @return The newly created document validity termination. Never <code>null</code>.
	 */
	public static DocumentValidityTermination create(final ChronoIndexDocument document,
			final long terminationTimestamp) {
		checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
		checkArgument(terminationTimestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(terminationTimestamp > document.getValidFromTimestamp(),
				"Precondition violation - document validity cannot be terminated before/at the documents creation timestamp!");
		return new DocumentValidityTerminationImpl(document, terminationTimestamp);
	}

	/**
	 * Returns the document to terminate the validity range for when executing this operation.
	 *
	 * @return The document to termintate the validity range for. Never <code>null</code>.
	 */
	public ChronoIndexDocument getDocument();

	/**
	 * Returns the termination timestamp, i.e. the new "valid to" timestamp for {@linkplain #getDocument() the document}.
	 *
	 * @return The new valid-to timestamp. Always positive, always greater than the "valid from" timestamp of the document.
	 */
	public long getTerminationTimestamp();

}
