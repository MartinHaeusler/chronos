package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * A step in the fluent graph query API.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <E>
 *            Either {@link Vertex} or {@link Edge}, depending on the result type of this query.
 */
public interface GraphFinalizableQueryBuilder<E extends Element> extends GraphQueryBaseBuilder<E, GraphFinalizableQueryBuilder<E>>, GraphQueryBuilderFinalizer<E> {

	/**
	 * Terminates the query, returning the number of elements that matched the query.
	 *
	 * @return The total number of elements that matched the query. Always greater than or equal to zero.
	 */
	public long count();

	/**
	 * Combines the current filter with the subsequent steps and connects them via a logical "and" operator.
	 *
	 * <p>
	 * Please note that the precedence order is:
	 * <ol>
	 * <li>{@link #not()}
	 * <li>{@link #and()}
	 * <li>{@link #or()}
	 * </ol>
	 *
	 * If you require a different ordering, please use {@link #begin()}-{@link #end()} braces.
	 *
	 * @return The next builder in the fluent API, for method chaining. Never <code>null</code>.
	 */
	public GraphQueryBuilder<E> and();

	/**
	 * Combines the current filter with the subsequent steps and connects them via a logical "or" operator.
	 *
	 * <p>
	 * Please note that the precedence order is:
	 * <ol>
	 * <li>{@link #not()}
	 * <li>{@link #and()}
	 * <li>{@link #or()}
	 * </ol>
	 *
	 * If you require a different ordering, please use {@link #begin()}-{@link #end()} braces.
	 *
	 * @return The next builder in the fluent API, for method chaining. Never <code>null</code>.
	 */
	public GraphQueryBuilder<E> or();

}
