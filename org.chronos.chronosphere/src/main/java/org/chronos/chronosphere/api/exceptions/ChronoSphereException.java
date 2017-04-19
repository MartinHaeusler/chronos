package org.chronos.chronosphere.api.exceptions;

import org.chronos.common.exceptions.ChronosException;

public class ChronoSphereException extends ChronosException {

	public ChronoSphereException() {
		super();
	}

	protected ChronoSphereException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoSphereException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoSphereException(final String message) {
		super(message);
	}

	public ChronoSphereException(final Throwable cause) {
		super(cause);
	}

}
