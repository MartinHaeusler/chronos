package org.chronos.chronodb.api.exceptions;

public class InvalidIndexAccessException extends ChronoDBIndexingException {

	public InvalidIndexAccessException() {
		super();
	}

	protected InvalidIndexAccessException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InvalidIndexAccessException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public InvalidIndexAccessException(final String message) {
		super(message);
	}

	public InvalidIndexAccessException(final Throwable cause) {
		super(cause);
	}

}
