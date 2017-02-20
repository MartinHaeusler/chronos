package org.chronos.chronodb.api;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * The mode in which duplicate versions are eliminated by {@link ChronoDB}.
 *
 * <p>
 * "Duplicated versions" occur when the same value for the same key is committed several times, without any other values
 * in between. In other words, if a commit on a key occurs, and the committed value is exactly the same as the previous
 * one, then a duplicate version is created. Duplicate versions are not "harmful" in and on themselves, but they also
 * offer no additional information - they merely add more data to the database, reducing query performance in the
 * process.
 *
 * <p>
 * This enumeration describes how {@link ChronoDB} should attempt to eliminate such duplicated versions.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public enum DuplicateVersionEliminationMode {

	/**
	 * Completely disables duplicate version elimination.
	 *
	 * <p>
	 * For a definition of the "Duplicate Version Elimination" process, please see
	 * {@link DuplicateVersionEliminationMode}.
	 *
	 * <p>
	 * This means that a commit will be treated "as is", without even attempting to detect duplicated versions. This
	 * results in the best write performance, but may reduce query performance and needlessly increase the size of the
	 * database, as duplicated versions provide no additional information value.
	 *
	 * <p>
	 * This setting is recommended under the following circumstances:
	 * <ul>
	 * <li>The write performance of a database or transaction has absolute priority over read performance.
	 * <li>The application logic which uses this ChronoDB instance performs duplicate version checks on its own.
	 * </ul>
	 */
	DISABLED("disabled", "off", "false"),

	/**
	 * Duplicate version elimination will be performed when {@link ChronoDBTransaction#commit()} is called.
	 *
	 * <p>
	 * For a definition of the "Duplicate Version Elimination" process, please see
	 * {@link DuplicateVersionEliminationMode}.
	 *
	 * <p>
	 * This introduces a performance penalty on the <code>commit()</code> operation, as every key-value pair that has
	 * been added in this transaction via {@link ChronoDBTransaction#put(String, Object)} will be checked for version
	 * duplicates.
	 *
	 */
	ON_COMMIT("onCommit", "commit", "on_commit", "true");

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	/** The primary name for this mode. */
	private final String primaryName;
	/** A set of aliases (alternative names) for this mode. */
	private final Set<String> aliases;
	/** The set of all possible names for this mode. The union of {@link #primaryName} and {@link #aliases}. */
	private final Set<String> allNames;

	/**
	 * Creates a new enum literal instance, for internal use only.
	 *
	 * @param primaryName
	 *            The primary name for the mode. Must not be <code>null</code>.
	 * @param aliases
	 *            The aliases (alternative names) to assign to this mode. May be empty, must not be <code>null</code>.
	 */
	private DuplicateVersionEliminationMode(final String primaryName, final String... aliases) {
		checkNotNull(primaryName, "Precondition violation - argument 'primaryName' must not be NULL!");
		this.primaryName = primaryName;
		Set<String> myAliases = Sets.newHashSet();
		if (aliases != null && aliases.length > 0) {
			for (String alias : aliases) {
				myAliases.add(alias);
			}
		}
		this.aliases = Collections.unmodifiableSet(myAliases);
		Set<String> myNames = Sets.newHashSet();
		myNames.add(primaryName);
		myNames.addAll(this.aliases);
		this.allNames = Collections.unmodifiableSet(myNames);
	}

	@Override
	public String toString() {
		return this.primaryName;
	}

	/**
	 * This method parses a string value into a {@link DuplicateVersionEliminationMode}.
	 *
	 * <p>
	 * This method takes all aliases into account and is therefore more fault tolerant than the default
	 * {@link #valueOf(String)} method.
	 *
	 * @param stringValue
	 *            The string value to parse. Must not be <code>null</code>.
	 * @return The Duplicate Version Elimination mode described in the string. Never <code>null</code>.
	 *
	 * @throws IllegalArgumentException
	 *             Thrown if the parsing process failed.
	 */
	public static DuplicateVersionEliminationMode fromString(final String stringValue) {
		checkNotNull(stringValue, "Precondition violation - argument 'stringValue' must not be NULL!");
		String token = stringValue.toLowerCase().trim();
		if (token.isEmpty()) {
			throw new IllegalArgumentException("Cannot parse DuplicateVersionEliminationMode from empty string!");
		}
		for (DuplicateVersionEliminationMode mode : DuplicateVersionEliminationMode.values()) {
			for (String name : mode.allNames) {
				if (name.equalsIgnoreCase(token)) {
					return mode;
				}
			}
		}
		throw new IllegalArgumentException("Unknown DuplicateVersionEliminationMode: '" + token + "'!");
	}
}
