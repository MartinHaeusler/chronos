package org.chronos.chronodb.api.exceptions;

public class ChronoDBBranchingException extends ChronoDBException {

	public ChronoDBBranchingException() {
		super();
	}

	public ChronoDBBranchingException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBBranchingException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBBranchingException(final String message) {
		super(message);
	}

	public ChronoDBBranchingException(final Throwable cause) {
		super(cause);
	}

}
