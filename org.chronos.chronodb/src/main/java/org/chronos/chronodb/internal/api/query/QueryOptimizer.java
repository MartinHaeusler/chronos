package org.chronos.chronodb.internal.api.query;

import org.chronos.chronodb.api.ChronoDB;

/**
 * A {@link QueryOptimizer} is an object responsible for rewriting a {@link ChronoDBQuery} into another query with equal
 * semantics but improved resource usage.
 *
 * <p>
 * Usually, this manifests itself in the fact that the returned optimized query produces the same result set as the
 * input query, but is likely to execute faster and/or is likely to uses fewer memory resources.
 *
 * <p>
 * Please note that an optimizer that simply returns the input query also fulfills the contract and is therefore a valid
 * (yet not very useful) optimizer.
 *
 * <p>
 * Query optimizer implementations are assumed to be <b>immutalbe</b> (in the ideal case <b>stateless</b>) and therefore
 * <b>thread-safe</b>!
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface QueryOptimizer {

	/**
	 * Optimizes the given query.
	 *
	 * <p>
	 * All implementations of this method are assumed to be <b>thread-safe</b> by {@link ChronoDB}!
	 *
	 * @param query
	 *            The query to optimize. Will not be modified. Must not be <code>null</code>.
	 * @return The optimized query. May be equal to the input query, may also be the same object (with respect to the
	 *         <code>==</code> operator). Never <code>null</code>.
	 */
	public ChronoDBQuery optimize(ChronoDBQuery query);

}
