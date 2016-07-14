package org.chronos.chronodb.api.exceptions;

public class ChronoDBSerializationException extends ChronoDBException {

	public ChronoDBSerializationException() {
		super();
	}

	public ChronoDBSerializationException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBSerializationException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBSerializationException(final String message) {
		super(message);
	}

	public ChronoDBSerializationException(final Throwable cause) {
		super(cause);
	}

}
