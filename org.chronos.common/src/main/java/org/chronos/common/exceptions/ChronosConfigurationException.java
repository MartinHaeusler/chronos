package org.chronos.common.exceptions;

public class ChronosConfigurationException extends ChronosException {

	protected ChronosConfigurationException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronosConfigurationException() {
		super();
	}

	public ChronosConfigurationException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronosConfigurationException(final String message) {
		super(message);
	}

	public ChronosConfigurationException(final Throwable cause) {
		super(cause);
	}

}
