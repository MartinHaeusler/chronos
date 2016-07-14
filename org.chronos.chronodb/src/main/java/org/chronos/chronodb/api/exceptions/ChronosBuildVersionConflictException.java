package org.chronos.chronodb.api.exceptions;

import org.chronos.common.exceptions.ChronosException;

public class ChronosBuildVersionConflictException extends ChronosException {

	public ChronosBuildVersionConflictException() {
		super();
	}

	protected ChronosBuildVersionConflictException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronosBuildVersionConflictException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronosBuildVersionConflictException(final String message) {
		super(message);
	}

	public ChronosBuildVersionConflictException(final Throwable cause) {
		super(cause);
	}

}
