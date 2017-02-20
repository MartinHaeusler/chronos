package org.chronos.chronograph.api.exceptions;

public class PropertyIsAlreadyIndexedException extends ChronoGraphIndexingException {

	public PropertyIsAlreadyIndexedException() {
		super();
	}

	protected PropertyIsAlreadyIndexedException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public PropertyIsAlreadyIndexedException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public PropertyIsAlreadyIndexedException(final String message) {
		super(message);
	}

	public PropertyIsAlreadyIndexedException(final Throwable cause) {
		super(cause);
	}

}
