package org.chronos.chronodb.internal.api.query;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.builder.query.QueryBuilderFinalizer;
import org.chronos.chronodb.api.builder.query.QueryBuilderStarter;

/**
 * The {@link QueryManager} is responsible for providing access to a number of objects related to queries in a given
 * {@link ChronoDB} instance.
 *
 * <p>
 * In particular, these objects include the following:
 * <ul>
 * <li>{@link QueryParser}: The query parser used for parsing {@link QueryTokenStream}s
 * <li>{@link QueryOptimizer}: The optimizer which is run on every {@link ChronoDBQuery} before executing it
 * <li>{@link QueryBuilderStarter}: The query builder implementation
 * </ul>
 *
 * All of the above interfaces require implementations that agree with each other and belong together. The query manager
 * acts as the container and/or factory for these implementation class instances.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface QueryManager {

	/**
	 * Returns the {@link QueryParser} implementation.
	 *
	 * <p>
	 * It is assumed that this method will constantly return the same object (with respect to the <code>==</code>
	 * operator).
	 *
	 * @return The query parser to be used. Never <code>null</code>.
	 */
	public QueryParser getQueryParser();

	/**
	 * Returns the {@link QueryOptimizer} implementation.
	 *
	 * <p>
	 * It is assumed that this method will constantly return the same object (with respect to the <code>==</code>
	 * operator).
	 *
	 * @return The query optimizer to be used. Never <code>null</code>.
	 */
	public QueryOptimizer getQueryOptimizer();

	/**
	 * Returns a new instance of the {@link QueryBuilderStarter} class associated with this query manager.
	 *
	 * <p>
	 * This is essentially a factory method for the concrete implementation of {@link QueryBuilderStarter}. It must
	 * return a <b>new</b> instance on every call.
	 *
	 *
	 * @param tx
	 *            The transaction on which the query is being built. Must not be <code>null</code>.
	 *
	 * @return A new instance of the query builder implementation.
	 */
	public QueryBuilderStarter createQueryBuilder(ChronoDBTransaction tx);

	/**
	 * Returns a new instance of the {@link QueryBuilderFinalizer} class associated with this query manager.
	 *
	 * <p>
	 * This is essentially a factory method for the concrete implementation of {@link QueryBuilderFinalizer}. It must
	 * return a <b>new</b> instance on every call.
	 *
	 * @param tx
	 *            The transaction on which the query is being executed. Must not be <code>null</code>.
	 * @param query
	 *            The query to execute on the transaction. Must not be <code>null</code>.
	 * 
	 * @return A new instance of the query builder finalizer implementation.
	 */
	public QueryBuilderFinalizer createQueryBuilderFinalizer(ChronoDBTransaction tx, ChronoDBQuery query);

}
