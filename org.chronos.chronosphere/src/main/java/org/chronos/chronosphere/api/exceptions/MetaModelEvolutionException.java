package org.chronos.chronosphere.api.exceptions;

public class MetaModelEvolutionException extends ChronoSphereException {

	public MetaModelEvolutionException() {
		super();
	}

	protected MetaModelEvolutionException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MetaModelEvolutionException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public MetaModelEvolutionException(final String message) {
		super(message);
	}

	public MetaModelEvolutionException(final Throwable cause) {
		super(cause);
	}

}
