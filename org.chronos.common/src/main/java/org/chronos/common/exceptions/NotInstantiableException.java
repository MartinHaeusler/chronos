package org.chronos.common.exceptions;

public class NotInstantiableException extends RuntimeException {

	public NotInstantiableException() {
		super();
	}

	public NotInstantiableException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NotInstantiableException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public NotInstantiableException(final String message) {
		super(message);
	}

	public NotInstantiableException(final Throwable cause) {
		super(cause);
	}

}
