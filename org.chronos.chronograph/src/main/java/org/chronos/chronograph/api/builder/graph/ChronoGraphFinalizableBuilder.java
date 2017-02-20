package org.chronos.chronograph.api.builder.graph;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.common.builder.ChronoBuilder;

/**
 * A builder for instances of {@link ChronoGraph}.
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
public interface ChronoGraphFinalizableBuilder<SELF extends ChronoGraphFinalizableBuilder<?>> extends ChronoBuilder<SELF> {

	/**
	 * Enables or disables the check for ID existence when adding a new graph element with a user-provided ID.
	 * 
	 * <p>
	 * For details, please refer to {@link ChronoGraphConfiguration#isCheckIdExistenceOnAddEnabled()}.
	 * 
	 * @param enableIdExistenceCheckOnAdd
	 *            Use <code>true</code> to enable the safety check, or <code>false</code> to disable it.
	 * 
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withIdExistenceCheckOnAdd(boolean enableIdExistenceCheckOnAdd);

	/**
	 * Enables or disables automatic opening of new graph transactions on-demand.
	 * 
	 * <p>
	 * For details, please refer to {@link ChronoGraphConfiguration#isTransactionAutoOpenEnabled()}.
	 * 
	 * @param enableAutoStartTransactions
	 *            Set this to <code>true</code> if auto-start of transactions should be enabled (default), otherwise set
	 *            it to <code>false</code> to disable this feature.
	 * 
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withTransactionAutoStart(boolean enableAutoStartTransactions);

	/**
	 * Enables the Chronos cache with the given maximum number of elements.
	 * 
	 * @param cacheSize
	 *            The size of the cache, in elements. Must be greater than zero.
	 * 
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withElementCacheOfSize(int cacheSize);

	/**
	 * Enables the Chronos query cache, with the given maximum number of queries.
	 * 
	 * <p>
	 * It is important to note that not every graph traversal triggers an index query in the underlying {@link ChronoDB}
	 * . Only gremlin queries that require access on a secondary index will be affected, and only the response of the
	 * secondary index will be cached.
	 * 
	 * 
	 * @param queryCacheSize
	 *            The number of index queries to cache. Must be greater than zero.
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withIndexQueryCacheOfSize(int queryCacheSize);

	/**
	 * Enables or disables blind overwrite protection.
	 *
	 * <p>
	 * If enabled, any commit that would overwrite a graph element pair written by a transaction with a higher timestamp
	 * will be rejected. In other words, if a commit has occurred on an entry, and this entry has never been visible to
	 * the current transaction, then this transaction is not allowed to "blindly overwrite" that entry.
	 *
	 * <p>
	 * This setting is enabled by default and it is recommended to keep it enabled. In general, this feature will cause
	 * some overhead on the {@linkplain ChronoDBTransaction#commit() commit} operation.
	 *
	 * <p>
	 * Corresponds to {@link ChronoDBConfiguration#ENABLE_BLIND_OVERWRITE_PROTECTION}.
	 *
	 * @param enableBlindOverwriteProtection
	 *            <code>true</code> to enable this feature, or <code>false</code> to disable it. Default is
	 *            <code>true</code>.
	 * @return <code>this</code>, for method chaining.
	 */
	public SELF withBlindOverwriteProtection(final boolean enableBlindOverwriteProtection);

	/**
	 * Builds the {@link ChronoGraph} instance, using the properties specified by the fluent API.
	 *
	 * <p>
	 * This method finalizes the build process. Afterwards, the builder should be discarded.
	 *
	 * @return The new {@link ChronoGraph} instance. Never <code>null</code>.
	 */
	public ChronoGraph build();

}