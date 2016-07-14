package org.chronos.chronodb.api.exceptions;

public class TransactionIsReadOnlyException extends ChronoDBTransactionException {

	public TransactionIsReadOnlyException() {
		super();
	}

	protected TransactionIsReadOnlyException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TransactionIsReadOnlyException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public TransactionIsReadOnlyException(final String message) {
		super(message);
	}

	public TransactionIsReadOnlyException(final Throwable cause) {
		super(cause);
	}

}
