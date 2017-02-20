package org.chronos.chronograph.internal.impl.structure.graph.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.chronos.chronograph.api.structure.ChronoGraph;

class ChronoVertexPropertyFeatures extends AbstractChronoGraphFeature implements Graph.Features.VertexPropertyFeatures {

	public ChronoVertexPropertyFeatures(final ChronoGraph graph) {
		super(graph);
	}

	// =====================================================================================
	// GENERAL
	// =====================================================================================

	@Override
	public boolean supportsProperties() {
		return true;
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
	public boolean supportsUserSuppliedIds() {
		return true;
	}

	@Override
	public boolean willAllowId(final Object id) {
		return id != null && id instanceof String;
	}

	// =====================================================================================
	// ADD / REMOVE
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
	// SUPPORTED DATA TYPES
	// =====================================================================================

	@Override
	public boolean supportsBooleanValues() {
		return true;
	}

	@Override
	public boolean supportsBooleanArrayValues() {
		return true;
	}

	@Override
	public boolean supportsByteValues() {
		return true;
	}

	@Override
	public boolean supportsByteArrayValues() {
		return true;
	}

	@Override
	public boolean supportsDoubleValues() {
		return true;
	}

	@Override
	public boolean supportsDoubleArrayValues() {
		return true;
	}

	@Override
	public boolean supportsFloatValues() {
		return true;
	}

	@Override
	public boolean supportsFloatArrayValues() {
		return true;
	}

	@Override
	public boolean supportsIntegerValues() {
		return true;
	}

	@Override
	public boolean supportsIntegerArrayValues() {
		return true;
	}

	@Override
	public boolean supportsLongValues() {
		return true;
	}

	@Override
	public boolean supportsLongArrayValues() {
		return true;
	}

	@Override
	public boolean supportsStringValues() {
		return true;
	}

	@Override
	public boolean supportsStringArrayValues() {
		return true;
	}

	@Override
	public boolean supportsMapValues() {
		return true;
	}

	@Override
	public boolean supportsMixedListValues() {
		return true;
	}

	@Override
	public boolean supportsUniformListValues() {
		return true;
	}

	@Override
	public boolean supportsSerializableValues() {
		return true;
	}

}