package org.chronos.chronograph.internal.impl.structure.graph.features;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;

abstract class AbstractChronoGraphFeature {

	private final ChronoGraphInternal graph;

	protected AbstractChronoGraphFeature(final ChronoGraphInternal graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.graph = graph;
	}

	protected ChronoGraphInternal getGraph() {
		return this.graph;
	}
}