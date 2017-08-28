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
public interface GraphQueryBuilder<E extends Element> extends GraphQueryBaseBuilder<E, GraphQueryBuilder<E>> {

	/**
	 * Starts a new filter definition.
	 *
	 * <p>
	 * Subsequent steps will define the details of the filter.
	 *
	 * @param property
	 *            The property to filter. Must not be <code>null</code>.
	 * 
	 * @return The next builder in the fluent API. Never <code>null</code>.
	 */
	public GraphWhereBuilder<E> where(String property);

}
