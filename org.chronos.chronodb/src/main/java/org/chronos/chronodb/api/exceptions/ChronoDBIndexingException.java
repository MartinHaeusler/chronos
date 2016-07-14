package org.chronos.chronodb.api.exceptions;

public class ChronoDBIndexingException extends ChronoDBException {

	public ChronoDBIndexingException() {
		super();
	}

	protected ChronoDBIndexingException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBIndexingException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBIndexingException(final String message) {
		super(message);
	}

	public ChronoDBIndexingException(final Throwable cause) {
		super(cause);
	}

}
