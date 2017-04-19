package org.chronos.chronosphere.api.exceptions.emf;

public class NameResolutionException extends ChronoSphereEMFException {

	public NameResolutionException() {
		super();
	}

	protected NameResolutionException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NameResolutionException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public NameResolutionException(final String message) {
		super(message);
	}

	public NameResolutionException(final Throwable cause) {
		super(cause);
	}

}
