package org.chronos.chronograph.internal.impl.index;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.IChronoGraphVertexIndex;
import org.chronos.common.annotation.PersistentClass;

/**
 * This class represents the definition of a vertex index (index metadata) on disk.
 *
 * @deprecated Superseded by {@link ChronoGraphVertexIndex2}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@Deprecated
@PersistentClass("kryo")
public class ChronoGraphVertexIndex extends AbstractChronoGraphIndex implements IChronoGraphVertexIndex {

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

	@Override
	public Indexer<?> createIndexer() {
		return new VertexRecordPropertyIndexer(this.getIndexedProperty());
	}

}
