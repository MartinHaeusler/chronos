package org.chronos.chronodb.api.exceptions;

public class IndexerConflictException extends ChronoDBIndexingException {

	public IndexerConflictException() {
		super();
	}

	protected IndexerConflictException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public IndexerConflictException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public IndexerConflictException(final String message) {
		super(message);
	}

	public IndexerConflictException(final Throwable cause) {
		super(cause);
	}

}
