package org.chronos.chronosphere.api.exceptions;

public class ChronoSphereConfigurationException extends ChronoSphereException {

	public ChronoSphereConfigurationException() {
		super();
	}

	protected ChronoSphereConfigurationException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoSphereConfigurationException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoSphereConfigurationException(final String message) {
		super(message);
	}

	public ChronoSphereConfigurationException(final Throwable cause) {
		super(cause);
	}

}
