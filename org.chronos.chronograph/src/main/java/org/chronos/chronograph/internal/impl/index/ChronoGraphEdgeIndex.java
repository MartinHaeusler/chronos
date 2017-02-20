package org.chronos.chronograph.internal.impl.index;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.chronos.chronograph.internal.ChronoGraphConstants;

public class ChronoGraphEdgeIndex extends AbstractChronoGraphIndex {

	protected ChronoGraphEdgeIndex() {
		// default constructor for serialization
	}

	public ChronoGraphEdgeIndex(final String indexedProperty) {
		super(indexedProperty);
	}

	@Override
	public String getBackendIndexKey() {
		return ChronoGraphConstants.INDEX_PREFIX_EDGE + this.getIndexedProperty();
	}

	@Override
	public Class<Edge> getIndexedElementClass() {
		return Edge.class;
	}

	@Override
	public String toString() {
		return "Index[Edge, " + this.getIndexedProperty() + "]";
	}

}
