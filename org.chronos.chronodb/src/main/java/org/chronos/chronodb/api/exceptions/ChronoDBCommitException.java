package org.chronos.chronodb.api.exceptions;

public class ChronoDBCommitException extends ChronoDBTransactionException {

	public ChronoDBCommitException() {
		super();
	}

	public ChronoDBCommitException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBCommitException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBCommitException(final String message) {
		super(message);
	}

	public ChronoDBCommitException(final Throwable cause) {
		super(cause);
	}

}
