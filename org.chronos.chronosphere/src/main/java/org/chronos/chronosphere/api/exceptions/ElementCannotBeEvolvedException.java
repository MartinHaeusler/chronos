package org.chronos.chronosphere.api.exceptions;

public class ElementCannotBeEvolvedException extends MetaModelEvolutionException {

	public ElementCannotBeEvolvedException() {
		super();
	}

	protected ElementCannotBeEvolvedException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ElementCannotBeEvolvedException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ElementCannotBeEvolvedException(final String message) {
		super(message);
	}

	public ElementCannotBeEvolvedException(final Throwable cause) {
		super(cause);
	}

}
