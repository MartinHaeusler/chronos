package org.chronos.chronograph.api.exceptions;

public class ChronoGraphCommitConflictException extends ChronoGraphException {

	public ChronoGraphCommitConflictException() {
		super();
	}

	protected ChronoGraphCommitConflictException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoGraphCommitConflictException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoGraphCommitConflictException(final String message) {
		super(message);
	}

	public ChronoGraphCommitConflictException(final Throwable cause) {
		super(cause);
	}

}
