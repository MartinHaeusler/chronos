package org.chronos.chronodb.api.builder.query;

/**
 * This is the initial interface that starts off the fluent query builder API.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface QueryBuilderStarter {

	/**
	 * Adds a keyspace constraint.
	 *
	 * <p>
	 * Without such a constraint, all keyspaces are scanned.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().inKeyspace("MyKeyspace").where("Hello").isEqualTo("World").getResult();
	 * </pre>
	 *
	 * @param keyspace
	 *            The keyspace to scan. Must not be <code>null</code>.
	 *
	 * @return The builder for method chaining.
	 */
	public QueryBuilder inKeyspace(String keyspace);

	/**
	 * Adds a constraint to scan only the default keyspace.
	 *
	 * <p>
	 * Without such a constraint, all keyspaces are scanned.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().inDefaultKeyspace().where("Hello").isEqualTo("World").getResult();
	 * </pre>
	 *
	 * @return The builder for method chaining.
	 */
	public QueryBuilder inDefaultKeyspace();

}
