package org.chronos.chronodb.internal.impl;

import java.util.UUID;

public class MatrixUtils {

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected MatrixUtils() {
		throw new IllegalStateException("MatrixMap must not be instantiated!");
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	/**
	 * Generates and returns a random name for a matrix map.
	 *
	 * @return A random matrix map name. Never <code>null</code>.
	 */
	public static String generateRandomName() {
		return "MATRIX_" + UUID.randomUUID().toString().replace("-", "_");
	}

	/**
	 * Checks if the given string is a valid matrix map name.
	 *
	 * @param mapName
	 *            The map name to check. May be <code>null</code>.
	 * @return <code>true</code> if the given map name is a valid matrix map name, or <code>false</code> if it is syntactically invalid or <code>null</code>.
	 */
	public static boolean isValidMatrixTableName(final String mapName) {
		if (mapName == null) {
			return false;
		}
		String tablenNameRegex = "MATRIX_[a-zA-Z0-9_]+";
		return mapName.matches(tablenNameRegex);
	}

	/**
	 * Asserts that the given map name is a valid matrix map name.
	 *
	 * <p>
	 * If the map name matches the syntax, this method does nothing. Otherwise, an exception is thrown.
	 *
	 * @param mapName
	 *            The map name to verify.
	 *
	 * @throws NullPointerException
	 *             Thrown if the map name is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             Thrown if the map name is no syntactically valid matrix table name.
	 *
	 * @see #isValidMatrixTableName(String)
	 */
	public static void assertIsValidMatrixTableName(final String mapName) {
		if (mapName == null) {
			throw new IllegalArgumentException("NULL is no valid Matrix Map name!");
		}
		if (isValidMatrixTableName(mapName) == false) {
			throw new IllegalArgumentException("The map name '" + mapName + "' is no valid Matrix Map name!");
		}
	}

}