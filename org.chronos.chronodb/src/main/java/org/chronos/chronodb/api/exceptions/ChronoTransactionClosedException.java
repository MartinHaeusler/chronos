package org.chronos.chronodb.api.exceptions;

public class ChronoTransactionClosedException extends ChronoDBTransactionException {

	public ChronoTransactionClosedException() {
		super();
	}

	public ChronoTransactionClosedException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoTransactionClosedException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoTransactionClosedException(final String message) {
		super(message);
	}

	public ChronoTransactionClosedException(final Throwable cause) {
		super(cause);
	}

}
