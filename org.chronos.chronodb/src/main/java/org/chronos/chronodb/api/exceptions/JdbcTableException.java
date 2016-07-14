package org.chronos.chronodb.api.exceptions;

public class JdbcTableException extends RuntimeException {

	public JdbcTableException() {
		super();
	}

	public JdbcTableException(final String message) {
		super(message);
	}

	public JdbcTableException(final Throwable cause) {
		super(cause);
	}

	public JdbcTableException(final String message, final Throwable cause) {
		super(message, cause);
	}

	protected JdbcTableException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
