package org.chronos.chronodb.api.builder.query;

/**
 * A fluent API for building queries in ChronoDB.
 *
 * <p>
 * Please see the individual methods for usage examples.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface QueryBuilder extends QueryBaseBuilder<QueryBuilder> {

	/**
	 * Begins a "where" clause. The supplied argument is the property which will be matched.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isEqualTo("Martin").getResult();
	 * </pre>
	 *
	 * @param property
	 *            The property to match. Must not be <code>null</code>.
	 *
	 * @return The builder to specify the condition in. Never <code>null</code>.
	 */
	public WhereBuilder where(String property);

}
