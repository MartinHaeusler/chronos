package org.chronos.chronograph.internal.impl.structure.graph.features;

import static com.google.common.base.Preconditions.*;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import org.chronos.chronograph.api.structure.ChronoGraph;

class ChronoEdgeFeatures implements Graph.Features.EdgeFeatures {

	private final ChronoGraphEdgePropertyFeatures propertyFeatures;

	public ChronoEdgeFeatures(final ChronoGraph graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.propertyFeatures = new ChronoGraphEdgePropertyFeatures(graph);
	}

	// =====================================================================================
	// ID HANDLING
	// =====================================================================================

	@Override
	public boolean supportsCustomIds() {
		return false;
	}

	@Override
	public boolean supportsAnyIds() {
		return false;
	}

	@Override
	public boolean supportsUserSuppliedIds() {
		return true;
	}

	@Override
	public boolean supportsNumericIds() {
		return false;
	}

	@Override
	public boolean supportsStringIds() {
		return true;
	}

	@Override
	public boolean supportsUuidIds() {
		return false;
	}

	@Override
	public boolean willAllowId(final Object id) {
		return id != null && id instanceof String;
	}

	// =====================================================================================
	// ADD EDGE / REMOVE EDGE
	// =====================================================================================

	@Override
	public boolean supportsAddEdges() {
		return true;
	}

	@Override
	public boolean supportsRemoveEdges() {
		return true;
	}

	// =====================================================================================
	// EDGE PROPERTIES
	// =====================================================================================

	@Override
	public boolean supportsAddProperty() {
		return true;
	}

	@Override
	public boolean supportsRemoveProperty() {
		return true;
	}

	@Override
	public EdgePropertyFeatures properties() {
		return this.propertyFeatures;
	}

}