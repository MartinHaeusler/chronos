package org.chronos.chronodb.api;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.impl.ChangeSetEntryImpl;

/**
 * Represents a single entry in the set of changes made by a {@link ChronoDBTransaction} before committing.
 *
 * <p>
 * Instances of this interface are always held in-memory. Upon commit, they are forwarded to the data store to persist the changes.
 *
 * <p>
 * Instances of this interface are simple data structures with no logic, and are assumed to be immutable.
 *
 * <p>
 * Instances of this interface <b>must</b> implement {@link #hashCode()} and {@link #equals(Object)} based only on the QualifiedKey.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChangeSetEntry {

	/**
	 * Creates a regular change instance, i.e. any change that is not a deletion.
	 *
	 * <p>
	 * For deletions, please use {@link #createDeletion(QualifiedKey)} instead.
	 *
	 * @param key
	 *            The key that was modified (inserted or altered). Must not be <code>null</code>.
	 * @param newValue
	 *            The new value that is assigned to the given key. Must not be <code>null</code>.
	 * @param options
	 *            The options to store alongside the change. May be empty, must not be <code>null</code>.
	 * @return The newly created change set entry. Never <code>null</code>.
	 */
	public static ChangeSetEntry createChange(final QualifiedKey key, final Object newValue, final PutOption... options) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(newValue, "Precondition violation - argument 'newValue' must not be NULL!");
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		return ChangeSetEntryImpl.createChange(key, newValue, options);
	}

	/**
	 * Creates a new deletion instance.
	 *
	 * <p>
	 * For insertions or modifications, please use {@link #createChange(QualifiedKey, Object, PutOption...)} instead.
	 *
	 * @param key
	 *            The key to be marked as deleted. Must not be <code>null</code>.
	 * @return The newly created change set entry. Never <code>null</code>.
	 */
	public static ChangeSetEntry createDeletion(final QualifiedKey key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return ChangeSetEntryImpl.createDeletion(key);
	}

	/**
	 * Returns the key that was changed.
	 *
	 * @return The qualified key. Never <code>null</code>.
	 */
	public String getKey();

	/**
	 * Returns the name of the keyspace where the change occurred.
	 *
	 * @return The name of the keyspace. Never <code>null</code>.
	 */
	public String getKeyspace();

	/**
	 * Returns the new value associated with the key.
	 *
	 * <p>
	 * If this method returns <code>null</code>, it indicates the deletion of the key.
	 *
	 * @return The value, or <code>null</code> if the key was deleted.
	 */
	public Object getValue();

	/**
	 * Returns the (put-)options for this change.
	 *
	 * @return The immutable set of options. Never <code>null</code>. May be empty.
	 */
	public Set<PutOption> getOptions();

	/**
	 * Checks if this change entry corresponds to a "set" operation that changed the value of a key.
	 *
	 * <p>
	 * The term "set" is interpreted here as in "set key X to value Y", where X and Y are non-<code>null</code>. It is mutually exclusive with {@link #isRemove()}.
	 *
	 * @return <code>true</code> if this is a "set" operation, otherwise <code>false</code>.
	 */
	public boolean isSet();

	/**
	 * Checks if this change entry corresponds to a "remove" operation that removed a key altogether.
	 *
	 * <p>
	 * The term "remove" is interpreted here as in "remove key X", where X is non-<code>null</code>. It is mutually exclusive with {@link #isSet()}.
	 *
	 * @return <code>true</code> if this is a "remove" operation, otherwise <code>false</code>.
	 */
	public boolean isRemove();

}
