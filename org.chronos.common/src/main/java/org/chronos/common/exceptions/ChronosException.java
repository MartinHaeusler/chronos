package org.chronos.common.exceptions;

public class ChronosException extends RuntimeException {

	public ChronosException() {
		super();
	}

	public ChronosException(final String message) {
		super(message);
	}

	public ChronosException(final Throwable cause) {
		super(cause);
	}

	public ChronosException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronosException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
