package org.chronos.chronograph.internal.impl.structure.graph.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronograph.api.structure.ChronoGraph;

public class ChronoGraphFeatures extends AbstractChronoGraphFeature implements Graph.Features {

	private final ChronoGraphGraphFeatures graphFeatures;
	private final ChronoVertexFeatures vertexFeatures;
	private final ChronoEdgeFeatures edgeFeatures;

	public ChronoGraphFeatures(final ChronoGraph graph) {
		super(graph);
		this.graphFeatures = new ChronoGraphGraphFeatures(graph);
		this.vertexFeatures = new ChronoVertexFeatures(graph);
		this.edgeFeatures = new ChronoEdgeFeatures(graph);
	}

	@Override
	public GraphFeatures graph() {
		return this.graphFeatures;
	}

	@Override
	public VertexFeatures vertex() {
		return this.vertexFeatures;
	}

	@Override
	public EdgeFeatures edge() {
		return this.edgeFeatures;
	}

	@Override
	public String toString() {
		return StringFactory.featureString(this);
	}

}
