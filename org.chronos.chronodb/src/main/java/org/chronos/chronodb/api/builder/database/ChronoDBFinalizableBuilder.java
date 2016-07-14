package org.chronos.chronodb.api.builder.database;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.common.builder.ChronoBuilder;

/**
 * A builder for instances of {@link ChronoDB}.
 *
 * <p>
 * When an instance of this interface is returned by the fluent builder API, then all information required for building
 * the database is complete, and {@link #build()} can be called to finalize the build process.
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
	 * Builds the {@link ChronoDB} instance, using the properties specified by the fluent API.
	 *
	 * <p>
	 * This method finalizes the build process. Afterwards, the builder should be discarded.
	 *
	 * @return The new {@link ChronoDB} instance. Never <code>null</code>.
	 */
	public ChronoDB build();

}
