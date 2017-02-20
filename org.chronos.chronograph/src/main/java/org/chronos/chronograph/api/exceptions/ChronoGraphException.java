package org.chronos.chronograph.api.exceptions;

import org.chronos.common.exceptions.ChronosException;

public class ChronoGraphException extends ChronosException {

	public ChronoGraphException() {
		super();
	}

	protected ChronoGraphException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoGraphException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoGraphException(final String message) {
		super(message);
	}

	public ChronoGraphException(final Throwable cause) {
		super(cause);
	}

}
