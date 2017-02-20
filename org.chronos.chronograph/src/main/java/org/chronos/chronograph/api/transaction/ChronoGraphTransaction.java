package org.chronos.chronograph.api.transaction;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.internal.impl.transaction.ElementLoadMode;
import org.chronos.chronograph.internal.impl.transaction.GraphTransactionContext;

import com.google.common.collect.Iterators;

public interface ChronoGraphTransaction {

	// =====================================================================================================================
	// TRANSACTION METADATA
	// =====================================================================================================================

	public ChronoGraph getGraph();

	public default long getTimestamp() {
		return this.getBackingDBTransaction().getTimestamp();
	}

	public default String getBranchName() {
		return this.getBackingDBTransaction().getBranchName();
	}

	public String getTransactionId();

	public long getRollbackCount();

	public boolean isThreadedTx();

	public boolean isThreadLocalTx();

	public boolean isOpen();

	// =====================================================================================================================
	// ELEMENT CREATION
	// =====================================================================================================================

	public ChronoVertex addVertex(Object... keyValues);

	public ChronoEdge addEdge(final ChronoVertex outVertex, final ChronoVertex inVertex, final String id,
			boolean isUserProvidedId, final String label, final Object... keyValues);

	// =====================================================================================================================
	// QUERY METHODS
	// =====================================================================================================================

	public Iterator<Vertex> vertices(Object... vertexIds);

	public Iterator<Vertex> getAllVerticesIterator();

	public default Iterator<Vertex> getVerticesIterator(final Iterable<String> chronoVertexIds) {
		checkNotNull(chronoVertexIds, "Precondition violation - argument 'chronoVertexIds' must not be NULL!");
		return this.getVerticesIterator(chronoVertexIds, ElementLoadMode.EAGER);
	}

	public Iterator<Vertex> getVerticesIterator(Iterable<String> chronoVertexIds, ElementLoadMode loadMode);

	public Iterator<Vertex> getVerticesByProperties(Map<String, String> propertyKeyToPropertyValue);

	public Set<Vertex> evaluateVertexQuery(final ChronoDBQuery query);

	public Iterator<Edge> edges(Object... edgeIds);

	public Iterator<Edge> getAllEdgesIterator();

	public Iterator<Edge> getEdgesIterator(Iterable<String> chronoEdgeIds);

	public Iterator<Edge> getEdgesByProperties(Map<String, String> propertyKeyToPropertyValue);

	public Set<Edge> evaluateEdgeQuery(final ChronoDBQuery query);

	public default Vertex getVertex(final String vertexId) {
		checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
		return this.getVertex(vertexId, ElementLoadMode.EAGER);
	}

	public default Vertex getVertex(final String vertexId, final ElementLoadMode loadMode) {
		checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
		checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
		Set<String> ids = Collections.singleton(vertexId);
		Iterator<Vertex> vertices = this.getVerticesIterator(ids, loadMode);
		return Iterators.getOnlyElement(vertices);
	}

	public default Edge getEdge(final String edgeId) {
		checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
		Set<String> ids = Collections.singleton(edgeId);
		Iterator<Edge> edges = this.getEdgesIterator(ids);
		return Iterators.getOnlyElement(edges);
	}

	// =====================================================================================================================
	// TEMPORAL QUERY METHODS
	// =====================================================================================================================

	Iterator<Long> getVertexHistory(Object vertexId);

	Iterator<Long> getVertexHistory(Vertex vertex);

	public Iterator<Long> getEdgeHistory(Object edgeId);

	public Iterator<Long> getEdgeHistory(Edge edge);

	public Iterator<Pair<Long, String>> getVertexModificationsBetween(long timestampLowerBound,
			long timestampUpperBound);

	public Iterator<Pair<Long, String>> getEdgeModificationsBetween(long timestampLowerBound, long timestampUpperBound);

	public Object getCommitMetadata(long commitTimestamp);

	// =====================================================================================================================
	// COMMIT & ROLLBACK
	// =====================================================================================================================

	public void commit();

	public void commit(Object metadata);

	public void commitIncremental();

	public void rollback();

	// =====================================================================================================================
	// CONTEXT & BACKING TRANSACTION
	// =====================================================================================================================

	public ChronoDBTransaction getBackingDBTransaction();

	public GraphTransactionContext getContext();

}
