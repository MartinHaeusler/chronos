package org.chronos.chronograph.internal.impl.index;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.IChronoGraphEdgeIndex;
import org.chronos.common.annotation.PersistentClass;

/**
 * This class represents the definition of an edge index (index metadata) on disk.
 *
 * @deprecated Superseded by {@link ChronoGraphEdgeIndex2}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@Deprecated
@PersistentClass("kryo")
public class ChronoGraphEdgeIndex extends AbstractChronoGraphIndex implements IChronoGraphEdgeIndex {

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

	@Override
	public Indexer<?> createIndexer() {
		return new EdgeRecordPropertyIndexer(this.getIndexedProperty());
	}

}
