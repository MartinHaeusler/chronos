package org.chronos.chronodb.internal.api.query;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;

/**
 * A {@link ChronoDBQuery} is a query that can be executed against a {@link ChronoDB} instance.
 *
 * <p>
 * Instances of classes implementing this interface are usually produced by feeding a {@link QueryTokenStream} to a
 * {@link QueryParser} via {@link QueryParser#parse(QueryTokenStream)}.
 *
 * <p>
 * Please note that all instances of classes that implement this interface are assumed to be <code>immutable</code>!
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBQuery {

	/**
	 * Returns the keyspace on which this query should be executed.
	 *
	 * @return The name of the keyspace. Never <code>null</code>.
	 */
	public String getKeyspace();

	/**
	 * Returns the root {@link QueryElement} in the Abstract Syntax Tree (AST) of the query.
	 *
	 * @return The AST root element. Never <code>null</code>.
	 */
	public QueryElement getRootElement();

}
