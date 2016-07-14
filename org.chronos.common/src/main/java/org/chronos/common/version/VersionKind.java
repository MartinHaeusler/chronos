package org.chronos.common.version;

import static com.google.common.base.Preconditions.*;

public enum VersionKind {

	// =====================================================================================================================
	// ENUM CONSTANTS
	// =====================================================================================================================

	SNAPSHOT("SNAPSHOT"), RELEASE("RELEASE");

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	public static VersionKind parse(final String string) {
		checkNotNull(string, "Precondition violation - argument 'string' must not be NULL!");
		String raw = string.toLowerCase().trim();
		for (VersionKind kind : VersionKind.values()) {
			if (kind.toString().equalsIgnoreCase(raw)) {
				return kind;
			}
		}
		throw new IllegalArgumentException("The given argument string is not a Version Kind: '" + string + "'!");
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String stringRepresentation;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private VersionKind(final String stringRepresentation) {
		this.stringRepresentation = stringRepresentation;
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	@Override
	public String toString() {
		return this.stringRepresentation;
	}

}
