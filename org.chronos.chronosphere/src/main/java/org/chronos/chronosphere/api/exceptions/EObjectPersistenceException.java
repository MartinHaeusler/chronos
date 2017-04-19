package org.chronos.chronosphere.api.exceptions;

public class EObjectPersistenceException extends ChronoSphereException {

	public EObjectPersistenceException() {
		super();
	}

	protected EObjectPersistenceException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public EObjectPersistenceException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public EObjectPersistenceException(final String message) {
		super(message);
	}

	public EObjectPersistenceException(final Throwable cause) {
		super(cause);
	}

}
