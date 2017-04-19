package org.chronos.chronosphere.api.exceptions.emf;

public class XMIConversionFailedException extends ChronoSphereEMFException {

	public XMIConversionFailedException() {
		super();
	}

	protected XMIConversionFailedException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public XMIConversionFailedException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public XMIConversionFailedException(final String message) {
		super(message);
	}

	public XMIConversionFailedException(final Throwable cause) {
		super(cause);
	}

}
