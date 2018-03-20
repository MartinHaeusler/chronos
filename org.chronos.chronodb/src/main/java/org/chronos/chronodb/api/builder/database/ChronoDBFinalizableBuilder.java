package org.chronos.chronodb.api.builder.database;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.common.builder.ChronoBuilder;

/**
 * A builder for instances of {@link ChronoDB}.
 *
 * <p>
 * When an instance of this interface is returned by the fluent builder API, then all information required for building
 * the database is complete, and {@link #build()} can be called to finalize the buildLRU process.
 *
 * <p>
 * Even though the {@link #build()} method becomes available at this stage, it is still possible to set properties
 * defined by the concrete implementations.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <SELF>
 *            The dynamic type of <code>this</code> to return for method chaining.
 */
public interface ChronoDBFinalizableBuilder<SELF extends ChronoDBFinalizableBuilder<?>> extends ChronoBuilder<SELF> {

	/**
	 * Enables Least-Recently-Used caching on the new {@link ChronoDB} instance.
	 *
	 * <p>
	 * If this operation is called several times on the same builder instance, the last setting wins.
	 *
	 * @param maxSize
	 *            The maximum number of elements to be contained in the LRU cache. If this number is less than or equal
	 *            to zero, the caching is disabled instead.
	 *
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withLruCacheOfSize(int maxSize);

	/**
	 * Enables or disables the assumption that values in the cache of this {@link ChronoDB} instance are immutable.
	 *
	 * <p>
	 * By default, this is set to <code>false</code>. Enabling this setting can greatly increase the speed of the cache,
	 * however cache state may be corrupted if cached values are changed by client code. This setting is only viable if
	 * the client ensures that the objects passed to the key-value store are effectively immutable.
	 *
	 * @param value
	 *            Set this to <code>true</code> to enable the assumption that cached value objects are immutable
	 *            (optimistic, faster, see above), otherwise use <code>false</code> (pessimistic, default).
	 *
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF assumeCachedValuesAreImmutable(boolean value);

	/**
	 * Enables Least-Recently-Used caching on the new {@link ChronoDB} instance for query results.
	 *
	 * @param maxSize
	 *            The maximum number of elements to be contained in the LRU Query Result Cache. If this number is less
	 *            than or equal to zero, the caching is disabled instead.
	 *
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withLruQueryCacheOfSize(int maxSize);

	/**
	 * Enables or disables duplicate version elimination on commit.
	 *
	 * <p>
	 * If this is enabled, every changed key-value pair will be checked for identity with the previous version.
	 * Key-value pairs that are identical to their predecessors will be filtered out and will not be committed. This
	 * does not alter the semantics of the store in any way, but it eliminates the duplicates, reducing overall store
	 * size on disk and enhancing read performance.
	 *
	 * <p>
	 * This setting is enabled by default and it is recommended to keep this setting enabled, unless the caller can
	 * guarantee that each version is different from the predecessor. In general, this feature will cause some overhead
	 * on the {@linkplain ChronoDBTransaction#commit() commit} operation.
	 *
	 * <p>
	 * Corresponds to {@link ChronoDBConfiguration#DUPLICATE_VERSION_ELIMINATION_MODE}.
	 *
	 * @param useDuplicateVersionElimination
	 *            <code>true</code> to enable this feature, or <code>false</code> to disable it. Default is
	 *            <code>true</code>.
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withDuplicateVersionElimination(final boolean useDuplicateVersionElimination);

	/**
	 * Specifies the {@link ConflictResolutionStrategy} to use for this database by default.
	 *
	 * <p>
	 * This setting can be overruled on a per-transaction basis.
	 *
	 * @param strategy
	 *            The strategy to apply, unless specified otherwise explicitly in a transaction. Must not be
	 *            <code>null</code>.
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withConflictResolutionStrategy(final ConflictResolutionStrategy strategy);

	/**
	 * Builds the {@link ChronoDB} instance, using the properties specified by the fluent API.
	 *
	 * <p>
	 * This method finalizes the buildLRU process. Afterwards, the builder should be discarded.
	 *
	 * @return The new {@link ChronoDB} instance. Never <code>null</code>.
	 */
	public ChronoDB build();

}
