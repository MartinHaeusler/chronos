package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;

/**
 * The first step in a {@link ChronoGraph#find() find} query.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface GraphQueryBuilderStarter {

	/**
	 * Defines that this query should return {@linkplain Vertex vertices}.
	 *
	 * @return The next step in the fluent API, for method chaining. Never <code>null</code>.
	 */
	public GraphQueryBuilder<Vertex> vertices();

	/**
	 * Defines that this query should return {@linkplain Edge edges}.
	 *
	 * @return The next step in the fluent API, for method chaining. Never <code>null</code>.
	 */
	public GraphQueryBuilder<Edge> edges();

}
