package org.chronos.chronodb.api.exceptions;

public class InvalidTransactionBranchException extends ChronoDBTransactionException {

	public InvalidTransactionBranchException() {
		super();
	}

	public InvalidTransactionBranchException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InvalidTransactionBranchException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public InvalidTransactionBranchException(final String message) {
		super(message);
	}

	public InvalidTransactionBranchException(final Throwable cause) {
		super(cause);
	}

}
