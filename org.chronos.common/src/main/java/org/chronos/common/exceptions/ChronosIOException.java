package org.chronos.common.exceptions;

public class ChronosIOException extends ChronosException {

	public ChronosIOException() {
		super();
	}

	protected ChronosIOException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronosIOException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronosIOException(final String message) {
		super(message);
	}

	public ChronosIOException(final Throwable cause) {
		super(cause);
	}

}
