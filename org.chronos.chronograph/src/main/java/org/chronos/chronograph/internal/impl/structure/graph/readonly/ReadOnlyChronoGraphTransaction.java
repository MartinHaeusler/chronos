package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Order;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.EdgeStatus;
import org.chronos.chronograph.api.transaction.GraphTransactionContext;
import org.chronos.chronograph.api.transaction.VertexStatus;
import org.chronos.chronograph.internal.impl.transaction.ElementLoadMode;

import java.util.Iterator;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphTransaction implements ChronoGraphTransaction {

    private final ChronoGraphTransaction currentTransaction;

    public ReadOnlyChronoGraphTransaction(final ChronoGraphTransaction currentTransaction) {
        checkNotNull(currentTransaction, "Precondition violation - argument 'currentTransaction' must not be NULL!");
        this.currentTransaction = currentTransaction;
    }


    @Override
    public ChronoGraph getGraph() {
        return new ReadOnlyChronoGraph(this.currentTransaction.getGraph());
    }

    @Override
    public long getTimestamp() {
        // this operation is allowed even in read-only mode;
        // this override circumvents the public call to "getBackingDBTransaction" (which is not permitted)
        return this.currentTransaction.getTimestamp();
    }

    @Override
    public String getBranchName() {
        // this operation is allowed even in read-only mode;
        // this override circumvents the public call to "getBackingDBTransaction" (which is not permitted)
        return this.currentTransaction.getBranchName();
    }

    @Override
    public String getTransactionId() {
        return this.currentTransaction.getTransactionId();
    }

    @Override
    public long getRollbackCount() {
        return this.currentTransaction.getRollbackCount();
    }

    @Override
    public boolean isThreadedTx() {
        return this.currentTransaction.isThreadedTx();
    }

    @Override
    public boolean isThreadLocalTx() {
        return this.currentTransaction.isThreadLocalTx();
    }

    @Override
    public boolean isOpen() {
        return this.currentTransaction.isOpen();
    }

    @Override
    public ChronoVertex addVertex(final Object... keyValues) {
        return this.unsupportedOperation();
    }

    @Override
    public ChronoEdge addEdge(final ChronoVertex outVertex, final ChronoVertex inVertex, final String id, final boolean isUserProvidedId, final String label, final Object... keyValues) {
        return this.unsupportedOperation();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return this.unmodifiableVertices(this.currentTransaction.vertices(vertexIds));
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Iterator<Vertex> getAllVerticesIterator() {
        return this.unmodifiableVertices(this.currentTransaction.getAllVerticesIterator());
    }

    @Override
    public Iterator<Vertex> getVerticesIterator(final Iterable<String> chronoVertexIds, final ElementLoadMode loadMode) {
        return this.unmodifiableVertices(this.currentTransaction.getVerticesIterator(chronoVertexIds, loadMode));
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return this.unmodifiableEdges(this.currentTransaction.edges(edgeIds));
    }

    @Override
    public Iterator<Edge> getAllEdgesIterator() {
        return this.unmodifiableEdges(this.currentTransaction.getAllEdgesIterator());
    }

    @Override
    public Iterator<Edge> getEdgesIterator(final Iterable<String> chronoEdgeIds, ElementLoadMode loadMode) {
        return this.unmodifiableEdges(this.currentTransaction.getEdgesIterator(chronoEdgeIds, loadMode));
    }

    @Override
    public VertexStatus vertexStatus(final String vertexId) {
        var status = this.currentTransaction.vertexStatus(vertexId);
        switch (status) {
            case VertexStatus.Exists exists -> {
                return new VertexStatus.Exists(
                    exists.lastModificationTimestamp(),
                    new ReadOnlyChronoVertex(exists.vertex())
                );
            }
            case VertexStatus.Deleted deleted -> {
                return deleted;
            }
            case VertexStatus.NotFound notFound -> {
                return notFound;
            }
        }
    }

    @Override
    public EdgeStatus edgeStatus(final String edgeId) {
        var status = this.currentTransaction.edgeStatus(edgeId);
        switch (status) {
            case EdgeStatus.Exists exists -> {
                return new EdgeStatus.Exists(
                    exists.lastModificationTimestamp(),
                    new ReadOnlyChronoEdge(exists.edge())
                );
            }
            case EdgeStatus.Deleted deleted -> {
                return deleted;
            }
            case EdgeStatus.NotFound notFound -> {
                return notFound;
            }
        }
    }

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        return Iterators.unmodifiableIterator(this.currentTransaction.getVertexHistory(vertexId, lowerBound, upperBound, order));
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        return Iterators.unmodifiableIterator(this.currentTransaction.getEdgeHistory(vertexId, lowerBound, upperBound, order));
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Edge edge) {
        return Iterators.unmodifiableIterator(this.currentTransaction.getEdgeHistory(edge));
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Vertex vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        return this.currentTransaction.getLastModificationTimestampOfVertex(vertex);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Object vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        return this.currentTransaction.getLastModificationTimestampOfVertex(vertexId);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        return this.currentTransaction.getLastModificationTimestampOfEdge(edge);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Object edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        return this.currentTransaction.getLastModificationTimestampOfEdge(edgeId);
    }

    @Override
    public Iterator<Pair<Long, String>> getVertexModificationsBetween(final long timestampLowerBound, final long timestampUpperBound) {
        return Iterators.unmodifiableIterator(this.currentTransaction.getVertexModificationsBetween(timestampLowerBound, timestampUpperBound));
    }

    @Override
    public Iterator<Pair<Long, String>> getEdgeModificationsBetween(final long timestampLowerBound, final long timestampUpperBound) {
        return Iterators.unmodifiableIterator(this.currentTransaction.getEdgeModificationsBetween(timestampLowerBound, timestampUpperBound));
    }

    @Override
    public Object getCommitMetadata(final long commitTimestamp) {
        return this.currentTransaction.getCommitMetadata(commitTimestamp);
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
        // transaction was read-only to begin with; nothing to do!
    }

    @Override
    public ChronoDBTransaction getBackingDBTransaction() {
        return this.unsupportedOperation();
    }

    @Override
    public GraphTransactionContext getContext() {
        return new ReadOnlyGraphTransactionContext(this.currentTransaction.getContext());
    }

    private <T> T unsupportedOperation() {
        throw new UnsupportedOperationException("This operation is not supported on a read-only graph!");
    }

    private Iterator<Vertex> unmodifiableVertices(Iterator<Vertex> vertices) {
        Iterator<Vertex> unmodifiableIterator = Iterators.unmodifiableIterator(vertices);
        return Iterators.transform(unmodifiableIterator, ReadOnlyChronoVertex::new);
    }

    private Iterator<Edge> unmodifiableEdges(Iterator<Edge> edges) {
        Iterator<Edge> unmodifiableIterator = Iterators.unmodifiableIterator(edges);
        return Iterators.transform(unmodifiableIterator, ReadOnlyChronoEdge::new);
    }
}
