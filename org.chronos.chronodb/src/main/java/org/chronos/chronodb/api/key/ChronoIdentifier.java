package org.chronos.chronodb.api.key;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.impl.temporal.ChronoIdentifierImpl;

/**
 * A {@link ChronoIdentifier} is a {@link TemporalKey} with additional metadata.
 *
 * <p>
 * Every entry in the entire {@link ChronoDB} can be uniquely identified by a ChronoIdentifier.
 *
 * <p>
 * This class does <b>not</b> extend {@link TemporalKey} or {@link QualifiedKey} due to problems with collection
 * containment checks and the {@link #equals(Object)} method.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoIdentifier /* NOT implements TemporalKey, QualifiedKey */ {

	// =====================================================================================================================
	// FACTORY METHODS
	// =====================================================================================================================

	/**
	 * Creates a new {@link ChronoIdentifier} instance.
	 *
	 * @param branch
	 *            The branch to use in the new identifier. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to use in the new identifier. Must not be negative.
	 * @param keyspace
	 *            The keyspace to use in the new identifier. Must not be <code>null</code>.
	 * @param key
	 *            The key to use in the new identifier. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link ChronoIdentifier} instance, filled with the data from the parameters. Never
	 *         <code>null</code>.
	 */
	public static ChronoIdentifier create(final Branch branch, final long timestamp, final String keyspace,
			final String key) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new ChronoIdentifierImpl(branch.getName(), keyspace, key, timestamp);
	}

	/**
	 * Creates a new {@link ChronoIdentifier} instance.
	 *
	 * @param branchName
	 *            The branch name to use in the new identifier. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to use in the new identifier. Must not be negative.
	 * @param keyspace
	 *            The keyspace to use in the new identifier. Must not be <code>null</code>.
	 * @param key
	 *            The key to use in the new identifier. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link ChronoIdentifier} instance, filled with the data from the parameters. Never
	 *         <code>null</code>.
	 */
	public static ChronoIdentifier create(final String branchName, final long timestamp, final String keyspace,
			final String key) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		return new ChronoIdentifierImpl(branchName, keyspace, key, timestamp);
	}

	/**
	 * Creates a new {@link ChronoIdentifier} instance.
	 *
	 * @param branchName
	 *            The branch name to use in the new identifier. Must not be <code>null</code>.
	 * @param temporalKey
	 *            The temporal key that contains the data to use in the new identifier. Data will be copied, the
	 *            temporal key remains unmodified. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link ChronoIdentifier} instance, filled with the data from the parameters. Never
	 *         <code>null</code>.
	 */
	public static ChronoIdentifier create(final String branchName, final TemporalKey temporalKey) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(temporalKey, "Precondition violation - argument 'temporalKey' must not be NULL!");
		return create(branchName, temporalKey.getTimestamp(), temporalKey.getKeyspace(), temporalKey.getKey());
	}

	/**
	 * Creates a new {@link ChronoIdentifier} instance.
	 *
	 * @param branch
	 *            The branch to use in the new identifier. Must not be <code>null</code>.
	 * @param temporalKey
	 *            The temporal key that contains the data to use in the new identifier. Data will be copied, the
	 *            temporal key remains unmodified. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link ChronoIdentifier} instance, filled with the data from the parameters. Never
	 *         <code>null</code>.
	 */
	public static ChronoIdentifier create(final Branch branch, final TemporalKey temporalKey) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(temporalKey, "Precondition violation - argument 'temporalKey' must not be NULL!");
		return create(branch, temporalKey.getTimestamp(), temporalKey.getKeyspace(), temporalKey.getKey());
	}

	/**
	 * Creates a new {@link ChronoIdentifier} instance.
	 *
	 * @param branchName
	 *            The branch name to use in the new identifier. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to use in the new identifier. Must not be negative.
	 * @param qualifiedKey
	 *            The qualified key that contains the data to use in the new identifier. Data will be copied, the
	 *            qualified key remains unmodified. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link ChronoIdentifier} instance, filled with the data from the parameters. Never
	 *         <code>null</code>.
	 */
	public static ChronoIdentifier create(final String branchName, final long timestamp,
			final QualifiedKey qualifiedKey) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(qualifiedKey, "Precondition violation - argument 'qualifiedKey' must not be NULL!");
		return create(branchName, timestamp, qualifiedKey.getKeyspace(), qualifiedKey.getKey());
	}

	/**
	 * Creates a new {@link ChronoIdentifier} instance.
	 *
	 * @param branch
	 *            The branch to use in the new identifier. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to use in the new identifier. Must not be negative.
	 * @param qualifiedKey
	 *            The qualified key that contains the data to use in the new identifier. Data will be copied, the
	 *            qualified key remains unmodified. Must not be <code>null</code>.
	 *
	 * @return The newly created {@link ChronoIdentifier} instance, filled with the data from the parameters. Never
	 *         <code>null</code>.
	 */
	public static ChronoIdentifier create(final Branch branch, final long timestamp, final QualifiedKey qualifiedKey) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(qualifiedKey, "Precondition violation - argument 'qualifiedKey' must not be NULL!");
		return create(branch, timestamp, qualifiedKey.getKeyspace(), qualifiedKey.getKey());
	}

	// =====================================================================================================================
	// GETTERS
	// =====================================================================================================================

	/**
	 * Returns the "key" part of this chrono identifer.
	 *
	 * @return The key. Never <code>null</code>.
	 */
	public String getKey();

	/**
	 * Returns the "keyspace" part of this chrono identifier.
	 *
	 * @return The keyspace name. Never <code>null</code>.
	 */
	public String getKeyspace();

	/**
	 * Returns the branch identifier for the branch in which this identifier resides.
	 *
	 * @return The branch identifier. Never <code>null</code>.
	 */
	public String getBranchName();

	/**
	 * Returns the timestamp associated with this chrono identifer.
	 *
	 * @return The timestamp
	 */
	public long getTimestamp();

	// =====================================================================================================================
	// CONVERSION METHODS
	// =====================================================================================================================

	/**
	 * Returns a representation of this {@link ChronoIdentifier} as a {@link TemporalKey}.
	 *
	 * <p>
	 * Note that a {@link TemporalKey} contains less data than a {@link ChronoIdentifier}. Two different identifiers may
	 * therefore produce two equal temporal keys.
	 *
	 * @return The temporal key representation of this identifier. Never <code>null</code>.
	 */
	public TemporalKey toTemporalKey();

	/**
	 * Returns a representation of this {@link ChronoIdentifier} as a {@link QualifiedKey}.
	 *
	 * <p>
	 * Note that a {@link QualifiedKey} contains less data than a {@link ChronoIdentifier}. Two different identifiers
	 * may therefore produce two equal qualified keys.
	 *
	 * @return The qualified key representation of this identifier. Never <code>null</code>.
	 */
	public QualifiedKey toQualifiedKey();
}
