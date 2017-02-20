package org.chronos.chronograph.api.exceptions;

public class ChronoGraphIndexingException extends ChronoGraphException {

	public ChronoGraphIndexingException() {
		super();
	}

	protected ChronoGraphIndexingException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoGraphIndexingException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoGraphIndexingException(final String message) {
		super(message);
	}

	public ChronoGraphIndexingException(final Throwable cause) {
		super(cause);
	}

}
