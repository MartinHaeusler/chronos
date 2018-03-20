package org.chronos.chronograph.internal.api.transaction;

import java.util.Collection;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecord;

public interface ChronoGraphTransactionInternal extends ChronoGraphTransaction {

	public Iterator<Vertex> getVerticesBySearchSpecifications(Collection<SearchSpecification<?>> searchSpecifications);

	public Iterator<Edge> getEdgesBySearchSpecifications(Collection<SearchSpecification<?>> searchSpecifications);

	public ChronoEdge loadIncomingEdgeFromEdgeTargetRecord(ChronoVertexImpl targetVertex, String label,
			EdgeTargetRecord record);

	public ChronoEdge loadOutgoingEdgeFromEdgeTargetRecord(ChronoVertexImpl sourceVertex, String label,
			EdgeTargetRecord record);

}
