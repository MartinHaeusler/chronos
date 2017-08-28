package org.chronos.chronodb.internal.impl.index.querycache;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.util.Quadruple;
import org.chronos.common.logging.ChronoLogger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;

/**
 * A simple query cache on a least-recently-used basis.
 *
 * <p>
 * Query results are keyed against a timestamp and a {@link SearchSpecification}. Note that this simple cache does not support sharing query results across many timestamps.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class LRUIndexQueryCache implements ChronoIndexQueryCache {

	private final Cache<Quadruple<Long, Branch, String, SearchSpecification<?>>, Set<String>> cache;

	public LRUIndexQueryCache(final int maxSize, final boolean recordStatistics) {
		CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().maximumSize(maxSize);
		if (recordStatistics) {
			builder = builder.recordStats();
		}
		this.cache = builder.build();
	}

	@Override
	public Set<String> getOrCalculate(final long timestamp, final Branch branch, final String keyspace,
			final SearchSpecification<?> searchSpec, final Callable<Set<String>> loadingFunction) {
		try {
			Set<String> result = this.cache.get(Quadruple.of(timestamp, branch, keyspace, searchSpec), loadingFunction);
			return result;
		} catch (ExecutionException e) {
			ChronoLogger.logError("Failed to load result of '" + searchSpec + "' at timestamp " + timestamp + "!", e);
			return null;
		}
	}

	@Override
	public CacheStats getStats() {
		return this.cache.stats();
	}

	@Override
	public void clear() {
		this.cache.invalidateAll();
	}

}
