package org.chronos.chronosphere.internal.ogm.api;

import static com.google.common.base.Preconditions.*;

public enum VertexKind {

	EOBJECT("eObject"), ECLASS("eClass"), EATTRIBUTE("eAttribute"), EREFERENCE("eReference"), EPACKAGE(
			"ePackage"), EPACKAGE_REGISTRY("ePackageRegistry"), EPACKAGE_BUNDLE("ePackageBundle");

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final String literal;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	private VertexKind(final String literal) {
		checkNotNull(literal, "Precondition violation - argument 'literal' must not be NULL!");
		this.literal = literal;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public String toString() {
		return this.literal;
	}

	public static VertexKind fromString(final String literal) {
		checkNotNull(literal, "Precondition violation - argument 'literal' must not be NULL!");
		String string = literal.trim();
		for (VertexKind kind : VertexKind.values()) {
			if (kind.literal.equalsIgnoreCase(string)) {
				return kind;
			}
		}
		return null;
	}

}
