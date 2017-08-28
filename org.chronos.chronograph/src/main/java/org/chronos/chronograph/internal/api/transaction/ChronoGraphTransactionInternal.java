package org.chronos.chronograph.internal.api.transaction;

import java.util.Collection;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;

public interface ChronoGraphTransactionInternal extends ChronoGraphTransaction {

	public Iterator<Vertex> getVerticesBySearchSpecifications(Collection<SearchSpecification<?>> searchSpecifications);

	public Iterator<Edge> getEdgesBySearchSpecifications(Collection<SearchSpecification<?>> searchSpecifications);
}
