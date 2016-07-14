package org.chronos.chronodb.api.exceptions;

public class ChronoDBConfigurationException extends ChronoDBException {

	public ChronoDBConfigurationException() {
		super();
	}

	public ChronoDBConfigurationException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBConfigurationException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBConfigurationException(final String message) {
		super(message);
	}

	public ChronoDBConfigurationException(final Throwable cause) {
		super(cause);
	}

}
