package org.chronos.chronodb.api.exceptions;

public class InvalidTransactionTimestampException extends ChronoDBTransactionException {

	public InvalidTransactionTimestampException() {
		super();
	}

	public InvalidTransactionTimestampException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InvalidTransactionTimestampException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public InvalidTransactionTimestampException(final String message) {
		super(message);
	}

	public InvalidTransactionTimestampException(final Throwable cause) {
		super(cause);
	}

}
