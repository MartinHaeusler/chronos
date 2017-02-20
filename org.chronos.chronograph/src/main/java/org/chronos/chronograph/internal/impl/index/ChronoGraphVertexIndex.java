package org.chronos.chronograph.internal.impl.index;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.internal.ChronoGraphConstants;

public class ChronoGraphVertexIndex extends AbstractChronoGraphIndex {

	protected ChronoGraphVertexIndex() {
		// default constructor for serialization
	}

	public ChronoGraphVertexIndex(final String indexedProperty) {
		super(indexedProperty);
	}

	@Override
	public String getBackendIndexKey() {
		return ChronoGraphConstants.INDEX_PREFIX_VERTEX + this.getIndexedProperty();
	}

	@Override
	public Class<Vertex> getIndexedElementClass() {
		return Vertex.class;
	}

	@Override
	public String toString() {
		return "Index[Vertex, " + this.getIndexedProperty() + "]";
	}

}
