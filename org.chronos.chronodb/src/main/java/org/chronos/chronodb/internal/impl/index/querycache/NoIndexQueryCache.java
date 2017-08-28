package org.chronos.chronodb.internal.impl.index.querycache;

import java.util.Set;
import java.util.concurrent.Callable;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.exceptions.ChronoDBIndexingException;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import com.google.common.cache.CacheStats;

/**
 * This implementation of {@link ChronoIndexQueryCache} "fakes" a cache.
 *
 * <p>
 * The only purpose of this class is to act as a placeholder for the cache object when caching is disabled. It does nothing else than passing the given search specification to the loading function.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class NoIndexQueryCache implements ChronoIndexQueryCache {

	@Override
	public Set<String> getOrCalculate(final long timestamp, final Branch branch, final String keyspace,
			final SearchSpecification<?> searchSpec, final Callable<Set<String>> loadingFunction) {
		try {
			return loadingFunction.call();
		} catch (Exception e) {
			throw new ChronoDBIndexingException("Failed to perform index query!", e);
		}
	}

	@Override
	public CacheStats getStats() {
		return null;
	}

	@Override
	public void clear() {
		// nothing to do
	}
}
