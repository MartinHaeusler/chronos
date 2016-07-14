package org.chronos.chronodb.api.exceptions;

public class UnknownKeyspaceException extends ChronoDBException {

	public UnknownKeyspaceException() {
		super();
	}

	public UnknownKeyspaceException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public UnknownKeyspaceException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public UnknownKeyspaceException(final String message) {
		super(message);
	}

	public UnknownKeyspaceException(final Throwable cause) {
		super(cause);
	}

}
