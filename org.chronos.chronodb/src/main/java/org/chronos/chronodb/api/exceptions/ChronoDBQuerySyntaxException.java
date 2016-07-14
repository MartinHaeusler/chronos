package org.chronos.chronodb.api.exceptions;

public class ChronoDBQuerySyntaxException extends ChronoDBException {

	public ChronoDBQuerySyntaxException() {
		super();
	}

	protected ChronoDBQuerySyntaxException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ChronoDBQuerySyntaxException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ChronoDBQuerySyntaxException(final String message) {
		super(message);
	}

	public ChronoDBQuerySyntaxException(final Throwable cause) {
		super(cause);
	}

}
