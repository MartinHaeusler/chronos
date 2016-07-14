package org.chronos.chronodb.internal.impl.index.querycache;

import java.util.Set;
import java.util.concurrent.Callable;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.query.SearchSpecification;

import com.google.common.cache.CacheStats;

/**
 * The {@link ChronoIndexQueryCache} is used to cache the results of index queries in a {@link ChronoDB} instance.
 *
 * <p>
 * This cache is essentially just a mapping from <i>executed query</i> to <i>query result</i>.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoIndexQueryCache {

	/**
	 * Runs the given search through the cache.
	 *
	 * <p>
	 * If the result for the requested search is already cached, the cached result will be returned. Otherwise, the
	 * result will be calculated via the given <code>loadingFunction</code>, and automatically be cached before
	 * returning it.
	 * @param timestamp
	 *            The timestamp at which to execute the query. Must not be negative.
	 * @param branch
	 *            The branch to run the index query on. Must not be <code>null</code>.
	 * @param searchSpec
	 *            The search specification to fulfill. Must not be <code>null</code>.
	 * @param loadingFunction
	 *            The function to use in case that the cache doesn't contain the result of the given query.
	 *
	 * @return The result of the given search request. May be empty, but never <code>null</code>.
	 */
	public Set<ChronoIdentifier> getOrCalculate(final long timestamp, final Branch branch,
			final SearchSpecification searchSpec, final Callable<Set<ChronoIdentifier>> loadingFunction);

	/**
	 * Returns the statistics of this cache.
	 *
	 * <p>
	 * The statistics object is only available when the <code>recordStatistics</code> parameter of the constructor was
	 * set to <code>true</code>.
	 *
	 * @return The statistics, or <code>null</code> if recording is turned off.
	 */
	public CacheStats getStats();

	/**
	 * Clears this query cache.
	 */
	public void clear();

}
