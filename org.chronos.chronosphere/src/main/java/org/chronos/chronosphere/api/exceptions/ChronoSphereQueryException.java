package org.chronos.chronosphere.api.exceptions;

public class ChronoSphereQueryException extends ChronoSphereException {

	public ChronoSphereQueryException() {
		super();
	}

	protected ChronoSphereQueryException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoSphereQueryException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoSphereQueryException(final String message) {
		super(message);
	}

	public ChronoSphereQueryException(final Throwable cause) {
		super(cause);
	}

}
