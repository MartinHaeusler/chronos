package org.chronos.chronodb.api.exceptions;

public class ValueTypeMismatchException extends ChronoDBSerializationException {

	public ValueTypeMismatchException() {
		super();
	}

	public ValueTypeMismatchException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ValueTypeMismatchException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ValueTypeMismatchException(final String message) {
		super(message);
	}

	public ValueTypeMismatchException(final Throwable cause) {
		super(cause);
	}

}
