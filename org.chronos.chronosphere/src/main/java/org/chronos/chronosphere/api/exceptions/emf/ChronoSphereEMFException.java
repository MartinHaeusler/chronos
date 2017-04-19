package org.chronos.chronosphere.api.exceptions.emf;

import org.chronos.chronosphere.api.exceptions.ChronoSphereException;

public class ChronoSphereEMFException extends ChronoSphereException {

	public ChronoSphereEMFException() {
		super();
	}

	protected ChronoSphereEMFException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoSphereEMFException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoSphereEMFException(final String message) {
		super(message);
	}

	public ChronoSphereEMFException(final Throwable cause) {
		super(cause);
	}

}
