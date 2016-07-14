package org.chronos.chronodb.api.exceptions;

public class ChronoDBStorageBackendException extends ChronoDBException {

	public ChronoDBStorageBackendException() {
		super();
	}

	public ChronoDBStorageBackendException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBStorageBackendException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBStorageBackendException(final String message) {
		super(message);
	}

	public ChronoDBStorageBackendException(final Throwable cause) {
		super(cause);
	}

}
