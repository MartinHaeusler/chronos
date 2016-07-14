package org.chronos.chronodb.api.exceptions;

public class ChronoDBTransactionException extends ChronoDBException {

	public ChronoDBTransactionException() {
		super();
	}

	public ChronoDBTransactionException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBTransactionException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBTransactionException(final String message) {
		super(message);
	}

	public ChronoDBTransactionException(final Throwable cause) {
		super(cause);
	}

}
