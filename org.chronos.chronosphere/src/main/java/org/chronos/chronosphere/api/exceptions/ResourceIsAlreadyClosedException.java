package org.chronos.chronosphere.api.exceptions;

public class ResourceIsAlreadyClosedException extends ChronoSphereException {

	public ResourceIsAlreadyClosedException() {
		super();
	}

	protected ResourceIsAlreadyClosedException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ResourceIsAlreadyClosedException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ResourceIsAlreadyClosedException(final String message) {
		super(message);
	}

	public ResourceIsAlreadyClosedException(final Throwable cause) {
		super(cause);
	}

}
