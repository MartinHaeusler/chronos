package org.chronos.chronosphere.api.exceptions;

public class MetaModelEvolutionCanceledException extends MetaModelEvolutionException {

	public MetaModelEvolutionCanceledException() {
		super();
	}

	protected MetaModelEvolutionCanceledException(final String message, final Throwable cause,
			final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MetaModelEvolutionCanceledException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public MetaModelEvolutionCanceledException(final String message) {
		super(message);
	}

	public MetaModelEvolutionCanceledException(final Throwable cause) {
		super(cause);
	}

}
