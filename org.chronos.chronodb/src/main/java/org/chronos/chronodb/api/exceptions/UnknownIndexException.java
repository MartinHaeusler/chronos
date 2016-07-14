package org.chronos.chronodb.api.exceptions;

public class UnknownIndexException extends ChronoDBIndexingException {

	public UnknownIndexException() {
		super();
	}

	protected UnknownIndexException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public UnknownIndexException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public UnknownIndexException(final String message) {
		super(message);
	}

	public UnknownIndexException(final Throwable cause) {
		super(cause);
	}

}
