package org.chronos.chronodb.api.key;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.impl.temporal.TemporalKeyImpl;

/**
 * A {@link TemporalKey} is a {@link QualifiedKey} with additional temporal information.
 *
 * <p>
 * This class does <b>not</b> extend {@link QualifiedKey} due to problems with collection containment checks and the
 * {@link #equals(Object)} method.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface TemporalKey /* NOT extends QualifiedKey, */ {

	// =====================================================================================================================
	// STATIC FACTORY METHODS
	// =====================================================================================================================

	/**
	 * Creates a new {@link TemporalKey}.
	 *
	 * @param timestamp
	 *            The timestamp to use in the new temporal key. Must not be negative.
	 * @param keyspace
	 *            The keyspace to use in the new temporal key. Must not be <code>null</code>.
	 * @param key
	 *            The key to use in the new temporal key. Must not be <code>null</code>.
	 *
	 * @return The newly created temporal key. Never <code>null</code>.
	 */
	public static TemporalKey create(final long timestamp, final String keyspace, final String key) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new TemporalKeyImpl(timestamp, keyspace, key);
	}

	/**
	 * Creates a new {@link TemporalKey}.
	 *
	 * @param timestamp
	 *            The timestamp to use in the new temporal key. Must not be negative.
	 * @param qualifiedKey
	 *            The qualified key to read data from for the new temporal key. Data will be copied, the qualified key
	 *            will not be modified. Must not be <code>null</code>.
	 *
	 * @return The newly created temporal key. Never <code>null</code>.
	 */
	public static TemporalKey create(final long timestamp, final QualifiedKey qualifiedKey) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return create(timestamp, qualifiedKey.getKeyspace(), qualifiedKey.getKey());
	}

	/**
	 * Creates a new {@link TemporalKey}, with the minimum timestamp (zero).
	 *
	 * @param keyspace
	 *            The keyspace to use in the new temporal key. Must not be <code>null</code>.
	 * @param key
	 *            The key to use in the new temporal key. Must not be <code>null</code>.
	 *
	 * @return The newly created temporal key. Never <code>null</code>.
	 */
	public static TemporalKey createMinTime(final String keyspace, final String key) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new TemporalKeyImpl(0L, keyspace, key);
	}

	/**
	 * Creates a new {@link TemporalKey}, with the maximum timestamp ({@link Long#MAX_VALUE}).
	 *
	 * @param keyspace
	 *            The keyspace to use in the new temporal key. Must not be <code>null</code>.
	 * @param key
	 *            The key to use in the new temporal key. Must not be <code>null</code>.
	 *
	 * @return The newly created temporal key. Never <code>null</code>.
	 */
	public static TemporalKey createMaxTime(final String keyspace, final String key) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new TemporalKeyImpl(Long.MAX_VALUE, keyspace, key);
	}

	// =====================================================================================================================
	// GETTERS
	// =====================================================================================================================

	/**
	 * Returns the timestamp associated with this temporal key.
	 *
	 * @return The timestamp
	 */
	public long getTimestamp();

	/**
	 * Returns the "key" part of this temporal key.
	 *
	 * @return The key. Never <code>null</code>.
	 */
	public String getKey();

	/**
	 * Returns the "keyspace" part of this temporal key.
	 *
	 * @return The keyspace name. Never <code>null</code>.
	 */
	public String getKeyspace();

	// =====================================================================================================================
	// CONVERSION METHODS
	// =====================================================================================================================

	/**
	 * Returns a representation of this {@link TemporalKey} as a {@link QualifiedKey}.
	 *
	 * <p>
	 * Note that a {@link QualifiedKey} contains less data than a {@link TemporalKey}. Two different temporal keys may
	 * therefore produce two equal qualified keys.
	 *
	 * @return The qualified key representation of this temporal key. Never <code>null</code>.
	 */
	public QualifiedKey toQualifiedKey();
}
