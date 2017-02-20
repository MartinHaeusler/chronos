package org.chronos.chronograph.api.exceptions;

public class ChronoGraphConfigurationException extends ChronoGraphException {

	public ChronoGraphConfigurationException() {
		super();
	}

	protected ChronoGraphConfigurationException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoGraphConfigurationException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoGraphConfigurationException(final String message) {
		super(message);
	}

	public ChronoGraphConfigurationException(final Throwable cause) {
		super(cause);
	}

}
