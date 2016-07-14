package org.chronos.chronodb.api.exceptions;

import org.chronos.common.exceptions.ChronosException;

public class ChronoDBException extends ChronosException {

	public ChronoDBException() {
		super();
	}

	protected ChronoDBException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBException(final String message) {
		super(message);
	}

	public ChronoDBException(final Throwable cause) {
		super(cause);
	}

}
