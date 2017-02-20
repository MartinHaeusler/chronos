package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface GraphQueryBuilderStarter {

	public GraphQueryBuilder<Vertex> vertices();

	public GraphQueryBuilder<Edge> edges();

}
