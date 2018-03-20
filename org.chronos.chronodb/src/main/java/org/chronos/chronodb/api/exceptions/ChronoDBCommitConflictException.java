package org.chronos.chronodb.api.exceptions;

public class ChronoDBCommitConflictException extends ChronoDBCommitException {

	public ChronoDBCommitConflictException() {
		super();
	}

	protected ChronoDBCommitConflictException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBCommitConflictException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBCommitConflictException(final String message) {
		super(message);
	}

	public ChronoDBCommitConflictException(final Throwable cause) {
		super(cause);
	}

}
