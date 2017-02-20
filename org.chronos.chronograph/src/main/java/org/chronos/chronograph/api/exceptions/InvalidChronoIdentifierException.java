package org.chronos.chronograph.api.exceptions;

public class InvalidChronoIdentifierException extends ChronoGraphException {

	public InvalidChronoIdentifierException() {
		super();
	}

	protected InvalidChronoIdentifierException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InvalidChronoIdentifierException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public InvalidChronoIdentifierException(final String message) {
		super(message);
	}

	public InvalidChronoIdentifierException(final Throwable cause) {
		super(cause);
	}

}
