package org.chronos.chronodb.api.builder.query;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;

/**
 * Represents the (potentially) last step in the fluent query API.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface FinalizableQueryBuilder extends QueryBaseBuilder<FinalizableQueryBuilder>, QueryBuilderFinalizer {

	/**
	 * Returns the previously built query, in a stand-alone format.
	 *
	 * <p>
	 * The returned query is independent of the transaction and consequently independent of the transaction timestamp. This method should be used when the result of a query at different timestamps is of interest. The resulting query can be executed via {@link ChronoDBTransaction#find(ChronoDBQuery)}.
	 *
	 * @return The built query. Never <code>null</code>.
	 */
	public ChronoDBQuery toQuery();

	/**
	 * Executes the previously built query, returning the count of matching elements.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * long resultCount = tx.find().where("name").contains("hello").count();
	 * </pre>
	 *
	 * @return The count of keys matching the query. Never negative.
	 */
	public long count();

	/**
	 * Extends the query by adding a logical "and" operator.
	 *
	 * @return The next builder for method chaining. Never <code>null</code>.
	 */
	public QueryBuilder and();

	/**
	 * Extends the query by adding a logical "or" operator.
	 *
	 * @return The next builder for method chaining. Never <code>null</code>.
	 */
	public QueryBuilder or();
}
