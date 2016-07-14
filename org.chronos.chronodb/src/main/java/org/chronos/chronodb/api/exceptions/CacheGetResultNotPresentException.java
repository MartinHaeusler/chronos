package org.chronos.chronodb.api.exceptions;

public class CacheGetResultNotPresentException extends ChronoDBException {

	public CacheGetResultNotPresentException() {
		super();
	}

	protected CacheGetResultNotPresentException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public CacheGetResultNotPresentException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public CacheGetResultNotPresentException(final String message) {
		super(message);
	}

	public CacheGetResultNotPresentException(final Throwable cause) {
		super(cause);
	}

}
