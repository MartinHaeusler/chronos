package org.chronos.chronodb.internal.impl.index.diff;

import java.util.Set;

public interface IndexValueDiff {

	/**
	 * Returns the "old" (previous) value, i.e. the "left" side of the diff.
	 *
	 * <p>
	 * If the {@linkplain #getOldValue() old value} is <code>null</code> and the {@linkplain #getNewValue() new value}
	 * is non-<code>null</code>, then this diff represents an {@linkplain #isEntryAddition() entry addition}.
	 *
	 * @return The old value. May be <code>null</code>.
	 */
	public Object getOldValue();

	/**
	 * Returns the "new" (next) value, i.e. the "right" side of the diff.
	 *
	 * <p>
	 * If the {@linkplain #getOldValue() old value} is <code>null</code> and the {@linkplain #getNewValue() new value}
	 * is non-<code>null</code>, then this diff represents an {@linkplain #isEntryAddition() entry addition}.
	 *
	 * @return The new value. May be <code>null</code>.
	 */
	public Object getNewValue();

	/**
	 * Returns the value additions for the given index name.
	 *
	 * @param indexName
	 *            The index name to get the additions for. Must not be <code>null</code>.
	 *
	 * @return The value additions. Never <code>null</code>. May be empty if nothing was added to the given index.
	 */
	public Set<String> getAdditions(final String indexName);

	/**
	 * Returns the value removals for the given index name.
	 *
	 * @param indexName
	 *            The index name to get the removals for. Must not be <code>null</code>.
	 *
	 * @return The value removals. Never <code>null</code>. May be empty if nothing was removed from the given index.
	 */
	public Set<String> getRemovals(final String indexName);

	/**
	 * Returns the set of indices that have actual changes in this diff.
	 *
	 * <p>
	 * This is equivalent to the set of indices where the set of {@linkplain #getAdditions(String) additions} and/or the
	 * set of {@linkplain #getRemovals(String) removals} is non-empty.
	 *
	 * @return The set of names of changed indices. May be empty if this diff {@linkplain #isEmpty() is empty}, but
	 *         never <code>null</code>.
	 */
	public Set<String> getChangedIndices();

	/**
	 * Checks if there are changes in this diff for the index with the given name.
	 *
	 * @param indexName
	 *            The name of the index to check for changes. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if there are changes to the index with the given name, otherwise <code>false</code>.
	 */
	public boolean isIndexChanged(final String indexName);

	/**
	 * Checks if this diff is empty, i.e. does not contain any changes.
	 *
	 * <p>
	 * If this method returns <code>true</code>, then {@link #getChangedIndices()} all of the following methods will
	 * return the empty set:
	 * <ul>
	 * <li>{@link #getChangedIndices()}
	 * <li>{@link #getAdditions(String)} (for every non-<code>null</code> argument string)
	 * <li>{@link #getRemovals(String)} (for every non-<code>null</code> argument string)
	 * </ul>
	 *
	 * <p>
	 * Please note that any given diff can either be {@linkplain #isAdditive() additive}, {@linkplain #isSubtractive()
	 * subtractive}, {@linkplain #isMixed() mixed} or {@linkplain #isEmpty() empty}. Every diff will return
	 * <code>true</code> for exactly one of these states.
	 *
	 * @return <code>true</code> if this is an empty diff, otherwise <code>false</code>.
	 */
	public boolean isEmpty();

	/**
	 * Checks if this diff represents an entry addition, i.e. the {@linkplain #getOldValue() old value} is
	 * <code>null</code> and the {@linkplain #getNewValue() new value} is non-<code>null</code>.
	 *
	 * <p>
	 * Please note that {@link #isEntryAddition()} and {@link #isEntryRemoval()} will both return <code>false</code> if
	 * both the {@linkplain #getOldValue() old value} and the {@linkplain #getNewValue() new value} are
	 * <code>null</code>. In any other case, either {@link #isEntryAddition()} or {@link #isEntryRemoval()} will return
	 * <code>true</code>, but never both of them.
	 *
	 * @return <code>true</code> if this diff represents an entry addition, otherwise <code>false</code>.
	 */
	public default boolean isEntryAddition() {
		return this.getOldValue() == null && this.getNewValue() != null;
	}

	/**
	 * Checks if this diff represents an entry removal, i.e. the {@linkplain #getOldValue() old value} is non-
	 * <code>null</code> and the {@linkplain #getNewValue() new value} is <code>null</code>.
	 *
	 * <p>
	 * Please note that {@link #isEntryAddition()} and {@link #isEntryRemoval()} will both return <code>false</code> if
	 * both the {@linkplain #getOldValue() old value} and the {@linkplain #getNewValue() new value} are
	 * <code>null</code>. In any other case, either {@link #isEntryAddition()} or {@link #isEntryRemoval()} will return
	 * <code>true</code>, but never both of them.
	 *
	 * @return <code>true</code> if this diff represents an entry removal, otherwise <code>false</code>.
	 */
	public default boolean isEntryRemoval() {
		return this.getOldValue() != null && this.getNewValue() == null;
	}

	/**
	 * Checks if this diff represents an entry update, i.e. the {@linkplain #getOldValue() old value} is non-
	 * <code>null</code> and the {@linkplain #getNewValue() new value} is non-<code>null</code>.
	 *
	 * @return <code>true</code> if this diff represents an entry update, otherwise <code>false</code>.
	 */
	public default boolean isEntryUpdate() {
		return this.getOldValue() != null && this.getNewValue() != null;
	}

	/**
	 * Checks if all changes in this diff are additive (contains only additions, but no removals).
	 *
	 * <p>
	 * In other words, this method will return <code>true</code> if and only if all of the following conditions hold:
	 * <ul>
	 * <li>{@link #getChangedIndices()} returns a non-empty set
	 * <li>{@link #getAdditions(String)} will return a non-empty set for at least one index name
	 * <li>{@link #getRemovals(String)} will return the empty set, for all index names
	 * </ul>
	 *
	 * <p>
	 * Please note that any given diff can either be {@linkplain #isAdditive() additive}, {@linkplain #isSubtractive()
	 * subtractive}, {@linkplain #isMixed() mixed} or {@linkplain #isEmpty() empty}. Every diff will return
	 * <code>true</code> for exactly one of these states.
	 *
	 * @return <code>true</code> if this diff is additive, otherwise <code>false</code>.
	 *
	 * @see #isSubtractive()
	 * @see #isMixed()
	 * @see #isEmpty()
	 */
	public boolean isAdditive();

	/**
	 * Checks if all changes in this diff are subtractive (contains only removals, but no additions).
	 *
	 * <p>
	 * In other words, this method will return <code>true</code> if and only if all of the following conditions hold:
	 * <ul>
	 * <li>{@link #getChangedIndices()} returns a non-empty set
	 * <li>{@link #getAdditions(String)} will return the empty set, for all index names
	 * <li>{@link #getRemovals(String)} will return a non-empty set for at least one index name
	 * </ul>
	 *
	 * <p>
	 * Please note that any given diff can either be {@linkplain #isAdditive() additive}, {@linkplain #isSubtractive()
	 * subtractive}, {@linkplain #isMixed() mixed} or {@linkplain #isEmpty() empty}. Every diff will return
	 * <code>true</code> for exactly one of these states.
	 *
	 * @return <code>true</code> if this diff is subtractive, otherwise <code>false</code>.
	 *
	 * @see #isAdditive()
	 * @see #isMixed()
	 * @see #isEmpty()
	 */
	public boolean isSubtractive();

	/**
	 * Checks if the changes in this diff are mixed (contains additions as well as removals).
	 *
	 * <p>
	 * In other words, this method will return <code>true</code> if and only if all of the following conditions hold:
	 * <ul>
	 * <li>{@link #getChangedIndices()} returns a non-empty set
	 * <li>{@link #getAdditions(String)} will return a non-empty set for at least one index name
	 * <li>{@link #getRemovals(String)} will return a non-empty set for at least one index name
	 * </ul>
	 *
	 * <p>
	 * Please note that any given diff can either be {@linkplain #isAdditive() additive}, {@linkplain #isSubtractive()
	 * subtractive}, {@linkplain #isMixed() mixed} or {@linkplain #isEmpty() empty}. Every diff will return
	 * <code>true</code> for exactly one of these states.
	 *
	 * @return <code>true</code> if this diff is mixed, otherwise <code>false</code>.
	 *
	 * @see #isAdditive()
	 * @see #isSubtractive()
	 * @see #isEmpty()
	 */
	public boolean isMixed();

}
