package org.chronos.chronodb.api.exceptions;

public class BlindOverwriteException extends ChronoDBCommitException {

	public BlindOverwriteException() {
		super();
	}

	protected BlindOverwriteException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public BlindOverwriteException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public BlindOverwriteException(final String message) {
		super(message);
	}

	public BlindOverwriteException(final Throwable cause) {
		super(cause);
	}

}
