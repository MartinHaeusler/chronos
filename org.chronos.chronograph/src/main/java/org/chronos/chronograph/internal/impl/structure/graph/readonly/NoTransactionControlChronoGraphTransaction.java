package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Order;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.structure.record.IEdgeRecord;
import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;
import org.chronos.chronograph.api.structure.record.IVertexRecord;
import org.chronos.chronograph.api.transaction.EdgeStatus;
import org.chronos.chronograph.api.transaction.GraphTransactionContext;
import org.chronos.chronograph.api.transaction.VertexStatus;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.transaction.ElementLoadMode;

import java.util.Iterator;

import static com.google.common.base.Preconditions.*;

public class NoTransactionControlChronoGraphTransaction implements ChronoGraphTransactionInternal {

    private final ChronoGraphTransactionInternal tx;

    public NoTransactionControlChronoGraphTransaction(ChronoGraphTransactionInternal tx) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        this.tx = tx;
    }

    @Override
    public ChronoEdge loadIncomingEdgeFromEdgeTargetRecord(final ChronoVertexImpl targetVertex, final String label, final IEdgeTargetRecord record) {
        return this.tx.loadIncomingEdgeFromEdgeTargetRecord(targetVertex, label, record);
    }

    @Override
    public ChronoEdge loadOutgoingEdgeFromEdgeTargetRecord(final ChronoVertexImpl sourceVertex, final String label, final IEdgeTargetRecord record) {
        return this.tx.loadOutgoingEdgeFromEdgeTargetRecord(sourceVertex, label, record);
    }

    @Override
    public IVertexRecord loadVertexRecord(final String recordId) {
        return this.tx.loadVertexRecord(recordId);
    }

    @Override
    public IEdgeRecord loadEdgeRecord(final String recordId) {
        return this.tx.loadEdgeRecord(recordId);
    }

    @Override
    public ChronoGraph getGraph() {
        return new NoTransactionControlChronoGraph(this.tx.getGraph());
    }

    @Override
    public long getTimestamp() {
        // this operation is allowed even in read-only mode;
        // this override circumvents the public call to "getBackingDBTransaction" (which is not permitted)
        return this.tx.getTimestamp();
    }

    @Override
    public String getBranchName() {
        // this operation is allowed even in read-only mode;
        // this override circumvents the public call to "getBackingDBTransaction" (which is not permitted)
        return this.tx.getBranchName();
    }


    @Override
    public String getTransactionId() {
        return this.tx.getTransactionId();
    }

    @Override
    public long getRollbackCount() {
        return this.tx.getRollbackCount();
    }

    @Override
    public boolean isThreadedTx() {
        return this.tx.isThreadedTx();
    }

    @Override
    public boolean isThreadLocalTx() {
        return this.tx.isThreadLocalTx();
    }

    @Override
    public boolean isOpen() {
        return this.tx.isOpen();
    }

    @Override
    public ChronoVertex addVertex(final Object... keyValues) {
        return this.tx.addVertex(keyValues);
    }

    @Override
    public ChronoEdge addEdge(final ChronoVertex outVertex, final ChronoVertex inVertex, final String id, final boolean isUserProvidedId, final String label, final Object... keyValues) {
        return this.tx.addEdge(outVertex, inVertex, id, isUserProvidedId, label, keyValues);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return this.tx.vertices(vertexIds);
    }

    @Override
    public Iterator<Vertex> getAllVerticesIterator() {
        return this.tx.getAllVerticesIterator();
    }

    @Override
    public Iterator<Vertex> getVerticesIterator(final Iterable<String> chronoVertexIds, final ElementLoadMode loadMode) {
        return this.tx.getVerticesIterator(chronoVertexIds, loadMode);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return this.tx.edges(edgeIds);
    }

    @Override
    public Iterator<Edge> getAllEdgesIterator() {
        return this.tx.getAllEdgesIterator();
    }

    @Override
    public Iterator<Edge> getEdgesIterator(final Iterable<String> chronoEdgeIds, final ElementLoadMode loadMode) {
        return this.tx.getEdgesIterator(chronoEdgeIds, loadMode);
    }

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        return this.tx.getVertexHistory(vertexId, lowerBound, upperBound, order);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        return this.tx.getEdgeHistory(vertexId, lowerBound, upperBound, order);
    }

    @Override
    public VertexStatus vertexStatus(final String vertexId) {
        return this.tx.vertexStatus(vertexId);
    }

    @Override
    public EdgeStatus edgeStatus(final String edgeId) {
        return this.tx.edgeStatus(edgeId);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Vertex vertex) {
        return this.tx.getLastModificationTimestampOfVertex(vertex);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Object vertexId) {
        return this.tx.getLastModificationTimestampOfVertex(vertexId);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Edge edge) {
        return this.tx.getLastModificationTimestampOfEdge(edge);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Object edgeId) {
        return this.tx.getLastModificationTimestampOfEdge(edgeId);
    }

    @Override
    public Iterator<Pair<Long, String>> getVertexModificationsBetween(final long timestampLowerBound, final long timestampUpperBound) {
        return this.tx.getVertexModificationsBetween(timestampLowerBound, timestampUpperBound);
    }

    @Override
    public Iterator<Pair<Long, String>> getEdgeModificationsBetween(final long timestampLowerBound, final long timestampUpperBound) {
        return this.tx.getEdgeModificationsBetween(timestampLowerBound, timestampUpperBound);
    }

    @Override
    public Object getCommitMetadata(final long commitTimestamp) {
        return this.tx.getCommitMetadata(commitTimestamp);
    }

    @Override
    public long commit() {
        return this.unsupportedOperation();
    }

    @Override
    public long commit(final Object metadata) {
        return this.unsupportedOperation();
    }

    @Override
    public void commitIncremental() {
        this.unsupportedOperation();
    }

    @Override
    public void rollback() {
        this.unsupportedOperation();
    }

    @Override
    public ChronoDBTransaction getBackingDBTransaction() {
        return this.unsupportedOperation();
    }

    @Override
    public GraphTransactionContext getContext() {
        return this.tx.getContext();
    }

    private <T> T unsupportedOperation() {
        throw new UnsupportedOperationException("Opening and closing transactions is not supported on this graph instance.");
    }
}
