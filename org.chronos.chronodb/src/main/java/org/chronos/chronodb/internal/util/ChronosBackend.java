package org.chronos.chronodb.internal.util;

import static com.google.common.base.Preconditions.*;

public enum ChronosBackend {

	INMEMORY, JDBC, FILE;

	public static boolean isValidBackend(final String backend) {
		if (backend == null || backend.trim().isEmpty()) {
			return false;
		}
		String string = backend.trim().toLowerCase();
		for (ChronosBackend literal : ChronosBackend.values()) {
			if (literal.toString().equalsIgnoreCase(string)) {
				return true;
			}
		}
		return false;
	}

	public static ChronosBackend fromString(final String stringValue) {
		checkNotNull(stringValue, "Precondition violation - argument 'stringValue' must not be NULL!");
		String string = stringValue.trim().toLowerCase();
		for (ChronosBackend backend : ChronosBackend.values()) {
			if (backend.toString().equalsIgnoreCase(string)) {
				return backend;
			}
		}
		throw new IllegalArgumentException("The given String is not a ChronoBackend: '" + stringValue + "'!");
	}
}
