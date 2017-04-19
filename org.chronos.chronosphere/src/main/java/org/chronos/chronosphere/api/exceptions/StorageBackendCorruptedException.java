package org.chronos.chronosphere.api.exceptions;

public class StorageBackendCorruptedException extends ChronoSphereException {

	public StorageBackendCorruptedException() {
		super();
	}

	protected StorageBackendCorruptedException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public StorageBackendCorruptedException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public StorageBackendCorruptedException(final String message) {
		super(message);
	}

	public StorageBackendCorruptedException(final Throwable cause) {
		super(cause);
	}

}
