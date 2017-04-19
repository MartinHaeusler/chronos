package org.chronos.chronosphere.api.exceptions;

public class ChronoSphereCommitException extends ChronoSphereException {

	public ChronoSphereCommitException() {
		super();
	}

	protected ChronoSphereCommitException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoSphereCommitException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoSphereCommitException(final String message) {
		super(message);
	}

	public ChronoSphereCommitException(final Throwable cause) {
		super(cause);
	}

}
