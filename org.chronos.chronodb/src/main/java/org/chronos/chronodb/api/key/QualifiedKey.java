package org.chronos.chronodb.api.key;

import static com.google.common.base.Preconditions.*;

import java.io.Serializable;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.impl.temporal.QualifiedKeyImpl;

/**
 * A qualified key is a combination of a keyspace name and a key name.
 *
 * <p>
 * Implementations of this interface <b>must</b> implement {@link #hashCode()} and {@link #equals(Object)} based upon
 * the key and the keyspace name.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface QualifiedKey extends Serializable {

	// =====================================================================================================================
	// FACTORY METHODS
	// =====================================================================================================================

	/**
	 * Creates a new {@link QualifiedKey} in the {@link ChronoDBConstants#DEFAULT_KEYSPACE_NAME default keyspace}.
	 *
	 * @param key
	 *            The key to use for the new qualified key. Must not be <code>null</code>.
	 *
	 * @return The newly created qualified key. Never <code>null</code>.
	 */
	public static QualifiedKey createInDefaultKeyspace(final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new QualifiedKeyImpl(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, key);
	}

	/**
	 * Creates a new {@link QualifiedKey}.
	 *
	 * @param key
	 *            The key to use for the new qualified key. Must not be <code>null</code>.
	 * @param keyspace
	 *            The keyspace to use for the new qualified key. Must not be <code>null</code>.
	 *
	 * @return The newly created qualified key. Never <code>null</code>.
	 */
	public static QualifiedKey create(final String keyspace, final String key) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new QualifiedKeyImpl(keyspace, key);
	}

	// =====================================================================================================================
	// GETTERS
	// =====================================================================================================================

	/**
	 * Returns the "key" part of this qualified key.
	 *
	 * @return The key. Never <code>null</code>.
	 */
	public String getKey();

	/**
	 * Returns the "keyspace" part of this qualified key.
	 *
	 * @return The keyspace name. Never <code>null</code>.
	 */
	public String getKeyspace();

}
