package org.chronos.common.exceptions;

public class UnknownEnumLiteralException extends RuntimeException {

	private final String message;

	// =================================================================================================================
	// CONSTRUCTORS
	// =================================================================================================================

	public UnknownEnumLiteralException(final Object enumLiteral) {
		if (enumLiteral == null) {
			this.message = "Encountered enum literal NULL!";
		} else {
			String className = enumLiteral.getClass().getName();
			this.message = "Encountered unknown literal of enum class '" + className + "': '" + enumLiteral + "'!";
		}
	}

	// =================================================================================================================
	// GETTERS
	// =================================================================================================================

	@Override
	public String getMessage() {
		return this.message;
	}
}
