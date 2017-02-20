package org.chronos.chronograph.internal.impl.structure.graph.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.chronos.chronograph.api.structure.ChronoGraph;

class ChronoVertexFeatures extends AbstractChronoGraphFeature implements Graph.Features.VertexFeatures {

	private final ChronoVertexPropertyFeatures propertyFeatures;

	public ChronoVertexFeatures(final ChronoGraph graph) {
		super(graph);
		this.propertyFeatures = new ChronoVertexPropertyFeatures(graph);
	}

	// =====================================================================================
	// ID HANDLING
	// =====================================================================================

	@Override
	public boolean supportsCustomIds() {
		return false;
	}

	@Override
	public boolean supportsUserSuppliedIds() {
		return true;
	}

	@Override
	public boolean supportsStringIds() {
		return true;
	}

	@Override
	public boolean supportsAnyIds() {
		return false;
	}

	@Override
	public boolean supportsUuidIds() {
		return false;
	}

	@Override
	public boolean supportsNumericIds() {
		return false;
	}

	@Override
	public boolean willAllowId(final Object id) {
		return id != null && id instanceof String;
	}

	// =====================================================================================
	// ADD PROPERTY / REMOVE PROPERTY
	// =====================================================================================

	@Override
	public boolean supportsAddProperty() {
		return true;
	}

	@Override
	public boolean supportsRemoveProperty() {
		return true;
	}

	// =====================================================================================
	// ADD VERTEX / REMOVE VERTEX
	// =====================================================================================

	@Override
	public boolean supportsAddVertices() {
		return true;
	}

	@Override
	public boolean supportsRemoveVertices() {
		return true;
	}

	// =====================================================================================
	// META- & MULTI-PROPERTIES
	// =====================================================================================

	@Override
	public boolean supportsMetaProperties() {
		return true;
	}

	@Override
	public boolean supportsMultiProperties() {
		return false;
	}

	@Override
	public VertexPropertyFeatures properties() {
		return this.propertyFeatures;
	}

}