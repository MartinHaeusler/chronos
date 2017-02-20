package org.chronos.chronograph.internal.impl.structure.graph.features;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.api.structure.ChronoGraph;

abstract class AbstractChronoGraphFeature {

	private final ChronoGraph graph;

	protected AbstractChronoGraphFeature(final ChronoGraph graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.graph = graph;
	}

	protected ChronoGraph getGraph() {
		return this.graph;
	}
}