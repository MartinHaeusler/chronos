package org.chronos.chronograph.api.transaction;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Order;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.internal.impl.transaction.ElementLoadMode;

import java.util.*;

import static com.google.common.base.Preconditions.*;

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

    public Iterator<Edge> edges(Object... edgeIds);

    public Iterator<Edge> getAllEdgesIterator();

    public default Iterator<Edge> getEdgesIterator(final Iterable<String> chronoEdgeIds) {
        checkNotNull(chronoEdgeIds, "Precondition violation - argument 'chronoEdgeIds' must not be NULL!");
        return this.getEdgesIterator(chronoEdgeIds, ElementLoadMode.EAGER);
    }

    public Iterator<Edge> getEdgesIterator(Iterable<String> chronoEdgeIds, ElementLoadMode loadMode);

    public default Vertex getVertex(final String vertexId) throws NoSuchElementException {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        return this.getVertex(vertexId, ElementLoadMode.EAGER);
    }

    public default Vertex getVertexOrNull(final String vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        return this.getVertexOrNull(vertexId, ElementLoadMode.EAGER);
    }

    public default Vertex getVertex(final String vertexId, final ElementLoadMode loadMode) throws NoSuchElementException {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        Set<String> ids = Collections.singleton(vertexId);
        Iterator<Vertex> vertices = this.getVerticesIterator(ids, loadMode);
        return Iterators.getOnlyElement(vertices);
    }

    public default Vertex getVertexOrNull(final String vertexId, final ElementLoadMode loadMode){
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        Set<String> ids = Collections.singleton(vertexId);
        Iterator<Vertex> vertices = this.getVerticesIterator(ids, loadMode);
        if(!vertices.hasNext()){
            return null;
        }else{
            return Iterators.getOnlyElement(vertices);
        }
    }

    public default Edge getEdge(final String edgeId) throws NoSuchElementException {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        return this.getEdge(edgeId, ElementLoadMode.EAGER);
    }

    public default Edge getEdgeOrNull(final String edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        return this.getEdgeOrNull(edgeId, ElementLoadMode.EAGER);
    }

    public default Edge getEdge(final String edgeId, final  ElementLoadMode loadMode) throws NoSuchElementException {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        Set<String> ids = Collections.singleton(edgeId);
        Iterator<Edge> edges = this.getEdgesIterator(ids, loadMode);
        return Iterators.getOnlyElement(edges);
    }

    public default Edge getEdgeOrNull(final String edgeId, final  ElementLoadMode loadMode)  {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        Set<String> ids = Collections.singleton(edgeId);
        Iterator<Edge> edges = this.getEdgesIterator(ids, loadMode);
        if(!edges.hasNext()){
            return null;
        }
        return Iterators.getOnlyElement(edges);
    }

    public VertexStatus vertexStatus(final String vertexId);

    public EdgeStatus edgeStatus(final String edgeId);

    // =====================================================================================================================
    // TEMPORAL QUERY METHODS
    // =====================================================================================================================

    public default Iterator<Long> getVertexHistory(Object vertexId){
        return this.getVertexHistory(vertexId, 0, this.getTimestamp(), Order.DESCENDING);
    }

    public default Iterator<Long> getVertexHistory(Object vertexId, Order order){
        return this.getVertexHistory(vertexId, 0, this.getTimestamp(), order);
    }

    public default Iterator<Long> getVertexHistory(Object vertexId, long lowerBound, long upperBound){
        return this.getVertexHistory(vertexId, lowerBound, upperBound, Order.DESCENDING);
    }

    public Iterator<Long> getVertexHistory(Object vertexId, long lowerBound, long upperBound, Order order);

    public default Iterator<Long> getVertexHistory(Vertex vertex){
        return this.getVertexHistory(vertex.id(), 0, this.getTimestamp(), Order.DESCENDING);
    }

    public default Iterator<Long> getVertexHistory(Vertex vertex, long lowerBound, long upperBound){
        return this.getVertexHistory(vertex.id(), lowerBound, upperBound, Order.DESCENDING);
    }

    public default Iterator<Long> getVertexHistory(Vertex vertex, Order order){
        return this.getVertexHistory(vertex.id(), 0, this.getTimestamp(), order);
    }

    public default Iterator<Long> getVertexHistory(Vertex vertex, long lowerBound, long upperBound, Order order){
        return this.getVertexHistory(vertex.id(), lowerBound, upperBound, order);
    }

    public default Iterator<Long> getEdgeHistory(Object edgeId){
        return this.getEdgeHistory(edgeId, 0, this.getTimestamp(), Order.DESCENDING);
    }

    public default Iterator<Long> getEdgeHistory(Object vertexId, Order order){
        return this.getEdgeHistory(vertexId, 0, this.getTimestamp(), order);
    }

    public default Iterator<Long> getEdgeHistory(Object vertexId, long lowerBound, long upperBound){
        return this.getEdgeHistory(vertexId, lowerBound, upperBound, Order.DESCENDING);
    }

    public Iterator<Long> getEdgeHistory(Object vertexId, long lowerBound, long upperBound, Order order);

    public default Iterator<Long> getEdgeHistory(Edge edge){
        return this.getEdgeHistory(edge.id(), 0, this.getTimestamp(), Order.DESCENDING);
    }

    public default Iterator<Long> getEdgeHistory(Edge edge, Order order){
        return this.getEdgeHistory(edge.id(), 0, this.getTimestamp(), order);
    }

    public default Iterator<Long> getEdgeHistory(Edge edge, long lowerBound, long upperBound){
        return this.getEdgeHistory(edge.id(), lowerBound, upperBound, Order.DESCENDING);
    }

    public default Iterator<Long> getEdgeHistory(Edge edge, long lowerBound, long upperBound, Order order){
        return this.getEdgeHistory(edge.id(), lowerBound, upperBound, order);
    }

    public long getLastModificationTimestampOfVertex(Vertex vertex);

    public long getLastModificationTimestampOfVertex(Object vertexId);

    public long getLastModificationTimestampOfEdge(Edge edge);

    public long getLastModificationTimestampOfEdge(Object edgeId);

    public Iterator<Pair<Long, String>> getVertexModificationsBetween(long timestampLowerBound, long timestampUpperBound);

    public Iterator<Pair<Long, String>> getEdgeModificationsBetween(long timestampLowerBound, long timestampUpperBound);

    public Object getCommitMetadata(long commitTimestamp);

    // =====================================================================================================================
    // COMMIT & ROLLBACK
    // =====================================================================================================================

    public long commit();

    public long commit(Object metadata);

    public void commitIncremental();

    public void rollback();

    // =====================================================================================================================
    // CONTEXT & BACKING TRANSACTION
    // =====================================================================================================================

    public ChronoDBTransaction getBackingDBTransaction();

    public GraphTransactionContext getContext();

}
