package org.chronos.chronograph.internal.impl.transaction;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.exceptions.ChronoGraphCommitConflictException;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleStatus;
import org.chronos.chronograph.internal.impl.structure.graph.PropertyStatus;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecord;
import org.chronos.chronograph.internal.impl.structure.record.VertexRecord;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;
import org.chronos.chronograph.internal.impl.util.ChronoId;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.base.CCC;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.logging.LogLevel;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class StandardChronoGraphTransaction implements ChronoGraphTransaction, ChronoGraphTransactionInternal {

    private final String transactionId;
    private final ChronoGraphInternal graph;
    private final ChronoDBTransaction backendTransaction;
    private final GraphTransactionContext context;
    private final ChronoGraphQueryProcessor queryProcessor;

    private long rollbackCount;

    public StandardChronoGraphTransaction(final ChronoGraphInternal graph,
                                          final ChronoDBTransaction backendTransaction) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        checkNotNull(backendTransaction, "Precondition violation - argument 'backendTransaction' must not be NULL!");
        this.transactionId = UUID.randomUUID().toString();
        this.graph = graph;
        this.backendTransaction = backendTransaction;
        this.context = new GraphTransactionContext();
        this.rollbackCount = 0L;
        this.queryProcessor = new ChronoGraphQueryProcessor(this);
    }

    // =====================================================================================================================
    // METADATA
    // =====================================================================================================================

    @Override
    public ChronoDBTransaction getBackingDBTransaction() {
        return this.backendTransaction;
    }

    @Override
    public GraphTransactionContext getContext() {
        return this.context;
    }

    @Override
    public String getTransactionId() {
        return this.transactionId;
    }

    @Override
    public long getRollbackCount() {
        return this.rollbackCount;
    }

    @Override
    public ChronoGraph getGraph() {
        return this.graph;
    }

    @Override
    public boolean isThreadedTx() {
        // can be overridden in subclasses.
        return false;
    }

    @Override
    public boolean isThreadLocalTx() {
        // can be overridden in subclasses.
        return true;
    }

    @Override
    public boolean isOpen() {
        // can be overridden in subclasses.
        return true;
    }

    // =====================================================================================================================
    // COMMIT & ROLLBACK
    // =====================================================================================================================

    @Override
    public void commit() {
        this.commit(null);
    }

    @Override
    public void commit(final Object metadata) {
        try (AutoLock lock = this.graph.commitLock()) {
            boolean merged = false;
            // only try to merge if not in incremental commit mode
            if (this.getBackingDBTransaction().isInIncrementalCommitMode() == false) {
                merged = this.performGraphLevelMergeWithStoreState(metadata);
            }
            if (merged == false) {
                // merge not required, commit this transaction
                this.mapModifiedVerticesToChronoDB();
                this.mapModifiedEdgesToChronoDB();
                this.mapModifiedGraphVariablesToChronoDB();
                // commit the transaction
                this.getBackingDBTransaction().commit(metadata);
            } else {
                // we committed the merged transaction and are done here
                this.getBackingDBTransaction().rollback();
            }
            // clear the transaction context
            this.context.clear();
        }
    }

    @Override
    public void commitIncremental() {
        try (AutoLock lock = this.graph.commitLock()) {
            if (this.getBackingDBTransaction().isInIncrementalCommitMode() == false) {
                // we're not yet in incremental commit mode, assert that the timestamp is the latest
                ChronoGraph g = this.graph;
                if (g instanceof ChronoThreadedTransactionGraph) {
                    g = ((ChronoThreadedTransactionGraph) g).getOriginalGraph();
                }
                ChronoGraph currentStateGraph = g.tx().createThreadedTx(this.getBranchName());
                long now = currentStateGraph.getNow(this.getBranchName());
                if (now != this.getTimestamp()) {
                    throw new IllegalStateException("Cannot perform incremental commit: a concurrent transaction has performed a commit since this transaction was opened!");
                }
            }
            this.mapModifiedVerticesToChronoDB();
            this.mapModifiedEdgesToChronoDB();
            this.mapModifiedGraphVariablesToChronoDB();
            // commit the transaction
            this.getBackingDBTransaction().commitIncremental();
            // clear the transaction context
            this.context.clear();
            // we "treat" the incremental commit like a rollback, because
            // we want existing proxies to re-register themselves at the
            // context (which we just had to clear).
            this.rollbackCount++;
        }
    }

    @Override
    public void rollback() {
        this.context.clear();
        this.rollbackCount++;
        this.getBackingDBTransaction().rollback();
    }

    // =====================================================================================================================
    // QUERY METHODS
    // =====================================================================================================================

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (vertexIds == null || vertexIds.length <= 0) {
            // query all vertices... this is bad.
            ChronoLogger.logWarning("Query requires iterating over all vertices."
                    + " For better performance, use 'has(...)' clauses in your gremlin.");
            return this.getAllVerticesIterator();
        }
        if (this.areAllOfType(String.class, vertexIds)) {
            // retrieve some vertices by IDs
            List<String> chronoVertexIds = Lists.newArrayList(vertexIds).stream().map(id -> (String) id)
                    .collect(Collectors.toList());
            return this.getVerticesIterator(chronoVertexIds);
        }
        if (this.areAllOfType(Vertex.class, vertexIds)) {
            // vertices were passed as arguments -> extract their IDs and query them
            List<String> ids = Lists.newArrayList(vertexIds).stream().map(v -> ((String) ((Vertex) v).id()))
                    .collect(Collectors.toList());
            return this.getVerticesIterator(ids);
        }
        // in any other case, something wrong was passed as argument...
        throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
    }

    @Override
    public Iterator<Vertex> getAllVerticesIterator() {
        return this.queryProcessor.getAllVerticesIterator();
    }

    @Override
    public Iterator<Vertex> getVerticesIterator(final Iterable<String> chronoVertexIds,
                                                final ElementLoadMode loadMode) {
        checkNotNull(chronoVertexIds, "Precondition violation - argument 'chronoVertexIds' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        return this.queryProcessor.getVerticesIterator(chronoVertexIds, loadMode);
    }

    @Override
    public Iterator<Vertex> getVerticesByProperties(final Map<String, Object> propertyKeyToPropertyValue) {
        checkNotNull(propertyKeyToPropertyValue,
                "Precondition violation - argument 'propertyKeyToPropertyValue' must not be NULL!");
        return this.queryProcessor.getVerticesByProperties(propertyKeyToPropertyValue);
    }

    @Override
    public Iterator<Vertex> getVerticesBySearchSpecifications(
            final Collection<SearchSpecification<?>> searchSpecifications) {
        checkNotNull(searchSpecifications,
                "Precondition violation - argument 'searchSpecifications' must not be NULL!");
        return this.queryProcessor.getVerticesBySearchSpecifications(Sets.newLinkedHashSet(searchSpecifications));
    }

    @Override
    public Set<Vertex> evaluateVertexQuery(final ChronoDBQuery query) {
        checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
        return this.queryProcessor.evaluateVertexQuery(query);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (edgeIds == null || edgeIds.length <= 0) {
            // query all edges... this is bad.
            ChronoLogger.logWarning("Query requires iterating over all edges."
                    + " For better performance, use 'has(...)' clauses in your gremlin.");
            return this.getAllEdgesIterator();
        }
        if (this.areAllOfType(String.class, edgeIds)) {
            // retrieve some edges by IDs
            List<String> chronoEdgeIds = Lists.newArrayList(edgeIds).stream().map(id -> (String) id)
                    .collect(Collectors.toList());
            return this.getEdgesIterator(chronoEdgeIds);
        }
        if (this.areAllOfType(Edge.class, edgeIds)) {
            // edges were passed as arguments -> extract their IDs and query them
            List<String> ids = Lists.newArrayList(edgeIds).stream().map(e -> ((String) ((Edge) e).id()))
                    .collect(Collectors.toList());
            return this.getEdgesIterator(ids);
        }
        // in any other case, something wrong was passed as argument...
        throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
    }

    @Override
    public Iterator<Edge> getAllEdgesIterator() {
        return this.queryProcessor.getAllEdgesIterator();
    }

    @Override
    public Iterator<Edge> getEdgesIterator(final Iterable<String> edgeIds) {
        checkNotNull(edgeIds, "Precondition violation - argument 'edgeIds' must not be NULL!");
        return this.queryProcessor.getEdgesIterator(edgeIds);
    }

    @Override
    public Iterator<Edge> getEdgesByProperties(final Map<String, Object> propertyKeyToPropertyValue) {
        checkNotNull(propertyKeyToPropertyValue,
                "Precondition violation - argument 'propertyKeyToPropertyValue' must not be NULL!");
        return this.queryProcessor.getEdgesByProperties(propertyKeyToPropertyValue);
    }

    @Override
    public Iterator<Edge> getEdgesBySearchSpecifications(
            final Collection<SearchSpecification<?>> searchSpecifications) {
        checkNotNull(searchSpecifications,
                "Precondition violation - argument 'searchSpecifications' must not be NULL!");
        return this.queryProcessor.getEdgesBySearchSpecifications(Sets.newLinkedHashSet(searchSpecifications));
    }

    @Override
    public Set<Edge> evaluateEdgeQuery(final ChronoDBQuery query) {
        checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
        return this.queryProcessor.evaluateEdgeQuery(query);
    }

    // =====================================================================================================================
    // TEMPORAL QUERY METHODS
    // =====================================================================================================================

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        if (vertexId instanceof Vertex) {
            return this.getVertexHistory((Vertex) vertexId);
        }
        if (vertexId instanceof String) {
            return this.getVertexHistory((String) vertexId);
        }
        throw new IllegalArgumentException("The given object is no valid vertex id: " + vertexId);
    }

    @Override
    public Iterator<Long> getVertexHistory(final Vertex vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        ChronoVertex chronoVertex = (ChronoVertex) vertex;
        return this.getVertexHistory(chronoVertex.id());
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        if (edgeId instanceof Edge) {
            return this.getEdgeHistory((Edge) edgeId);
        }
        if (edgeId instanceof String) {
            return this.getEdgeHistory((String) edgeId);
        }
        throw new IllegalArgumentException("The given object is no valid edge id: " + edgeId);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        ChronoEdge chronoEdge = (ChronoEdge) edge;
        return this.getEdgeHistory(chronoEdge.id());
    }

    @Override
    public Iterator<Pair<Long, String>> getVertexModificationsBetween(final long timestampLowerBound,
                                                                      final long timestampUpperBound) {
        checkArgument(timestampLowerBound >= 0,
                "Precondition violation - argument 'timestampLowerBound' must not be negative!");
        checkArgument(timestampUpperBound >= 0,
                "Precondition violation - argument 'timestampUpperBound' must not be negative!");
        checkArgument(timestampLowerBound <= timestampUpperBound,
                "Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
        checkArgument(timestampLowerBound <= this.getTimestamp(),
                "Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
        checkArgument(timestampUpperBound <= this.getTimestamp(),
                "Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
        Iterator<TemporalKey> temporalKeyIterator = this.getBackingDBTransaction().getModificationsInKeyspaceBetween(
                ChronoGraphConstants.KEYSPACE_VERTEX, timestampLowerBound, timestampUpperBound);
        return Iterators.transform(temporalKeyIterator, tk -> Pair.of(tk.getTimestamp(), tk.getKey()));
    }

    @Override
    public Iterator<Pair<Long, String>> getEdgeModificationsBetween(final long timestampLowerBound,
                                                                    final long timestampUpperBound) {
        checkArgument(timestampLowerBound >= 0,
                "Precondition violation - argument 'timestampLowerBound' must not be negative!");
        checkArgument(timestampUpperBound >= 0,
                "Precondition violation - argument 'timestampUpperBound' must not be negative!");
        checkArgument(timestampLowerBound <= timestampUpperBound,
                "Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
        checkArgument(timestampLowerBound <= this.getTimestamp(),
                "Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
        checkArgument(timestampUpperBound <= this.getTimestamp(),
                "Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
        Iterator<TemporalKey> temporalKeyIterator = this.getBackingDBTransaction().getModificationsInKeyspaceBetween(
                ChronoGraphConstants.KEYSPACE_EDGE, timestampLowerBound, timestampUpperBound);
        return Iterators.transform(temporalKeyIterator, tk -> Pair.of(tk.getTimestamp(), tk.getKey()));
    }

    @Override
    public Object getCommitMetadata(final long commitTimestamp) {
        checkArgument(commitTimestamp >= 0,
                "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkArgument(commitTimestamp <= this.getTimestamp(),
                "Precondition violation - argument 'commitTimestamp' must not be larger than the transaction timestamp!");
        return this.getBackingDBTransaction().getCommitMetadata(commitTimestamp);
    }

    // =================================================================================================================
    // ELEMENT CREATION METHODS
    // =================================================================================================================

    @Override
    public ChronoVertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object id = ElementHelper.getIdValue(keyValues).orElse(null);
        boolean userProvidedId = true;
        if (id != null && id instanceof String == false) {
            throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        }
        if (id == null) {
            id = ChronoId.random();
            // we generated the ID ourselves, it did not come from the user
            userProvidedId = false;
        }
        String vertexId = (String) id;
        String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        if (userProvidedId && this.getGraph().getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled()) {
            // assert that we don't already have a graph element with this ID in our transaction cache
            ChronoVertex modifiedVertex = this.getContext().getModifiedVertex(vertexId);
            if (modifiedVertex != null) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(vertexId);
            }
            // assert that we don't already have a graph element with this ID in our persistence
            ChronoDBTransaction backingTx = this.getBackingDBTransaction();
            boolean vertexIdAlreadyExists = backingTx.exists(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId);
            if (vertexIdAlreadyExists) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(vertexId);
            }
        }
        this.logAddVertex(vertexId, userProvidedId);
        ChronoVertexImpl vertex = new ChronoVertexImpl(vertexId, this.graph, this, label);
        ElementHelper.attachProperties(vertex, keyValues);
        vertex.updateLifecycleStatus(ElementLifecycleStatus.PROPERTY_CHANGED);
        return this.context.getOrCreateVertexProxy(vertex);
    }

    @Override
    public ChronoEdge addEdge(final ChronoVertex outVertex, final ChronoVertex inVertex, final String id,
                              final boolean isUserProvidedId, final String label, final Object... keyValues) {
        if (isUserProvidedId && this.getGraph().getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled()) {
            // assert that we don't already have a graph element with this ID in our transaction cache
            ChronoEdge modifiedEdge = this.getContext().getModifiedEdge(id);
            if (modifiedEdge != null) {
                throw Graph.Exceptions.edgeWithIdAlreadyExists(id);
            }
            // assert that we don't already have a graph element with this ID in our persistence
            ChronoDBTransaction backingTx = this.getBackingDBTransaction();
            boolean edgeIdAlreadyExists = backingTx.exists(ChronoGraphConstants.KEYSPACE_EDGE, id);
            if (edgeIdAlreadyExists) {
                throw Graph.Exceptions.edgeWithIdAlreadyExists(id);
            }
        }
        // create the edge
        ChronoVertexImpl outV = ChronoProxyUtil.resolveVertexProxy(outVertex);
        ChronoVertexImpl inV = ChronoProxyUtil.resolveVertexProxy(inVertex);
        ChronoEdgeImpl edge = ChronoEdgeImpl.create(id, outV, label, inV);
        // set the properties (if any)
        if (keyValues != null && keyValues.length > 0) {
            for (int i = 0; i < keyValues.length; i += 2) {
                if (keyValues[i] instanceof T == false) {
                    String key = (String) keyValues[i];
                    Object value = keyValues[i + 1];
                    edge.property(key, value);
                }
            }
        }
        edge.updateLifecycleStatus(ElementLifecycleStatus.NEW);
        return this.context.getOrCreateEdgeProxy(edge);
    }

    // =====================================================================================================================
    // EQUALS & HASH CODE
    // =====================================================================================================================

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.transactionId == null ? 0 : this.transactionId.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        StandardChronoGraphTransaction other = (StandardChronoGraphTransaction) obj;
        if (this.transactionId == null) {
            if (other.transactionId != null) {
                return false;
            }
        } else if (!this.transactionId.equals(other.transactionId)) {
            return false;
        }
        return true;
    }

    // =====================================================================================================================
    // LOADING METHODS
    // =====================================================================================================================

    public ChronoVertexImpl loadVertex(final String id, final ElementLoadMode loadMode) {
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        // first, try to find the vertex in our 'modified vertices' cache
        ChronoVertexImpl modifiedVertex = this.context.getModifiedVertex(id);
        if (modifiedVertex != null) {
            // the given vertex is in our 'modified vertices' cache; reuse it
            return modifiedVertex;
        }
        // then, try to find it in our 'already loaded' cache
        ChronoVertexImpl loadedVertex = this.context.getLoadedVertexForId(id);
        if (loadedVertex != null) {
            // the vertex was already loaded in this transaction; return the same instance
            return loadedVertex;
        }
        ChronoVertexImpl vertex = null;
        switch (loadMode) {
            case EAGER:
                // we are not sure if there is a vertex in the database for the given id. We need
                // to make a load attempt to make sure it exists.
                ChronoDBTransaction tx = this.getBackingDBTransaction();
                VertexRecord record = tx.get(ChronoGraphConstants.KEYSPACE_VERTEX, id.toString());
                // load the vertex from the database
                if (record == null) {
                    return null;
                }
                vertex = new ChronoVertexImpl(this.graph, this, record);
                break;
            case LAZY:
                // we can trust that there actually IS a vertex in the database for the given ID,
                // but we want to load it lazily when a vertex property is first accessed.
                vertex = new ChronoVertexImpl(this.graph, this, id, Vertex.DEFAULT_LABEL, true);
                vertex.updateLifecycleStatus(ElementLifecycleStatus.PERSISTED);
                break;
            default:
                throw new UnknownEnumLiteralException(loadMode);
        }
        // register the loaded instance
        this.context.registerLoadedVertex(vertex);
        return vertex;
    }

    public ChronoEdgeImpl loadEdge(final String id) {
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        // first, try to find the edge in our 'modified edges' cache
        ChronoEdgeImpl modifiedEdge = this.context.getModifiedEdge(id);
        if (modifiedEdge != null) {
            // the given vertex is in our 'modified edges' cache; reuse it
            return modifiedEdge;
        }
        // then, try to find it in our 'already loaded' cache
        ChronoEdgeImpl loadedEdge = this.context.getLoadedEdgeForId(id);
        if (loadedEdge != null) {
            // the edge was already loaded in this transaction; return the same instance
            return loadedEdge;
        }
        // load the edge from the database
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        EdgeRecord record = tx.get(ChronoGraphConstants.KEYSPACE_EDGE, id.toString());
        if (record == null) {
            return null;
        }
        ChronoEdgeImpl edge = ChronoEdgeImpl.create(this.graph, this, record);
        // register the loaded edge
        this.context.registerLoadedEdge(edge);
        return edge;
    }

    @Override
    public ChronoEdge loadOutgoingEdgeFromEdgeTargetRecord(final ChronoVertexImpl sourceVertex, final String label,
                                                           final EdgeTargetRecord record) {
        checkNotNull(sourceVertex, "Precondition violation - argument 'sourceVertex' must not be NULL!");
        checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
        checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
        String id = record.getEdgeId();
        // first, try to find the edge in our 'modified edges' cache
        ChronoEdgeImpl modifiedEdge = this.context.getModifiedEdge(id);
        if (modifiedEdge != null) {
            // the given vertex is in our 'modified edges' cache; reuse it
            return modifiedEdge;
        }
        // then, try to find it in our 'already loaded' cache
        ChronoEdgeImpl loadedEdge = this.context.getLoadedEdgeForId(id);
        if (loadedEdge != null) {
            // the edge was already loaded in this transaction; return the same instance
            return loadedEdge;
        }
        // the edge was not found, create it
        ChronoEdgeImpl edge = ChronoEdgeImpl.outgoingEdgeFromRecord(sourceVertex, label, record);
        // register the loaded edge
        this.context.registerLoadedEdge(edge);
        return edge;
    }

    @Override
    public ChronoEdge loadIncomingEdgeFromEdgeTargetRecord(final ChronoVertexImpl targetVertex, final String label,
                                                           final EdgeTargetRecord record) {
        checkNotNull(targetVertex, "Precondition violation - argument 'targetVertex' must not be NULL!");
        checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
        checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
        String id = record.getEdgeId();
        // first, try to find the edge in our 'modified edges' cache
        ChronoEdgeImpl modifiedEdge = this.context.getModifiedEdge(id);
        if (modifiedEdge != null) {
            // the given vertex is in our 'modified edges' cache; reuse it
            return modifiedEdge;
        }
        // then, try to find it in our 'already loaded' cache
        ChronoEdgeImpl loadedEdge = this.context.getLoadedEdgeForId(id);
        if (loadedEdge != null) {
            // the edge was already loaded in this transaction; return the same instance
            return loadedEdge;
        }
        // the edge was not found, create it
        ChronoEdgeImpl edge = ChronoEdgeImpl.incomingEdgeFromRecord(targetVertex, label, record);
        // register the loaded edge
        this.context.registerLoadedEdge(edge);
        return edge;
    }

    // =====================================================================================================================
    // INTERNAL HELPER METHODS
    // =====================================================================================================================

    private boolean areAllOfType(final Class<?> clazz, final Object... objects) {
        for (Object object : objects) {
            if (clazz.isInstance(object) == false) {
                return false;
            }
        }
        return true;
    }

    private Iterator<Long> getVertexHistory(final String vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        return tx.history(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId);
    }

    private Iterator<Long> getEdgeHistory(final String edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        return tx.history(ChronoGraphConstants.KEYSPACE_EDGE, edgeId);
    }

    private boolean performGraphLevelMergeWithStoreState(final Object metadata) {
        // check if another transaction has been committed since this transaction was opened
        ChronoGraph g = this.graph;
        if (g instanceof ChronoThreadedTransactionGraph) {
            g = ((ChronoThreadedTransactionGraph) g).getOriginalGraph();
        }
        ChronoGraph currentStateGraph = g.tx().createThreadedTx(this.getBranchName());
        long now = currentStateGraph.getNow(this.getBranchName());
        if (now == this.getTimestamp()) {
            // nothing has happened in the backing store since this transaction was
            // opened, therefore no conflict resolution is needed.
            return false;
        }
        this.mergeVertexChangesInto(currentStateGraph);
        this.mergeEdgeChangesInto(currentStateGraph);
        this.mergeGraphVariableChangesInto(currentStateGraph);
        currentStateGraph.tx().commit(metadata);
        return true;
    }


    private void mergeVertexChangesInto(final ChronoGraph currentStateGraph) {
        Set<ChronoVertexImpl> verticesToSynchronize = Sets.newHashSet(this.getContext().getModifiedVertices());
        for (ChronoVertexImpl vertex : verticesToSynchronize) {
            ElementLifecycleStatus status = vertex.getStatus();
            switch (status) {
                case REMOVED: {
                    Vertex storeVertex = Iterators.getOnlyElement(currentStateGraph.vertices(vertex.id()), null);
                    if (storeVertex != null) {
                        // delete the vertex in the store
                        storeVertex.remove();
                    }
                    break;
                }
                case OBSOLETE: {
                    // ignore
                    break;
                }
                case NEW: {
                    // the vertex is new in the current transaction
                    Vertex storeVertex = Iterators.getOnlyElement(currentStateGraph.vertices(vertex.id()), null);
                    if (storeVertex == null) {
                        // create the vertex
                        storeVertex = currentStateGraph.addVertex(T.id, vertex.id());
                    }
                    // copy the properties from the new vertex to the store vertex
                    this.copyProperties(vertex, storeVertex);
                    break;
                }
                case EDGE_CHANGED: {
                    // ignore, edges are synchronized in another step
                    break;
                }
                case PROPERTY_CHANGED: {
                    // synchronize the properties
                    Vertex storeVertex = Iterators.getOnlyElement(currentStateGraph.vertices(vertex.id()), null);
                    if (storeVertex != null) {
                        Set<String> propertyKeys = Sets.newHashSet();
                        propertyKeys.addAll(vertex.keys());
                        propertyKeys.addAll(storeVertex.keys());
                        for (String propertyKey : propertyKeys) {
                            PropertyStatus propertyStatus = vertex.getPropertyStatus(propertyKey);
                            switch (propertyStatus) {
                                case NEW:
                                    // FALL THROUGH
                                case MODIFIED:
                                    VertexProperty<?> property = vertex.property(propertyKey);
                                    VertexProperty<?> storeProperty = storeVertex.property(propertyKey, property.value());
                                    this.copyMetaProperties(property, storeProperty);
                                    break;
                                case PERSISTED:
                                    // ignore, property is untouched in transaction
                                    break;
                                case REMOVED:
                                    // remove in store as well
                                    storeVertex.property(propertyKey).remove();
                                    break;
                                case UNKNOWN:
                                    // ignore, property is unknown in transaction
                                    break;
                                default:
                                    throw new UnknownEnumLiteralException(propertyStatus);
                            }
                        }
                    }
                    break;
                }
                case PERSISTED: {
                    break;
                }
                default:
                    throw new UnknownEnumLiteralException(status);
            }

        }
    }

    private void mergeEdgeChangesInto(final ChronoGraph currentStateGraph) {
        Set<ChronoEdgeImpl> edgesToSynchronize = Sets.newHashSet(this.getContext().getModifiedEdges());
        for (ChronoEdgeImpl edge : edgesToSynchronize) {
            ElementLifecycleStatus status = edge.getStatus();
            switch (status) {
                case NEW: {
                    // try to get the edge from the store
                    Edge storeEdge = Iterators.getOnlyElement(currentStateGraph.edges(edge.id()), null);
                    if (storeEdge == null) {
                        Vertex outV = Iterators.getOnlyElement(currentStateGraph.vertices(edge.outVertex().id()), null);
                        Vertex inV = Iterators.getOnlyElement(currentStateGraph.vertices(edge.inVertex().id()), null);
                        if (outV == null || inV == null) {
                            throw new IllegalArgumentException("This should never happen: an edge outV or inV has not been copied over to the current state graph!");
                        }
                        storeEdge = outV.addEdge(edge.label(), inV, T.id, edge.id());
                    } else {
                        // store edge already exists, check that the neighboring vertices are the same
                        if (Objects.equal(edge.inVertex().id(), storeEdge.inVertex().id()) == false || Objects.equal(edge.outVertex().id(), storeEdge.outVertex().id()) == false) {
                            throw new ChronoGraphCommitConflictException("There is an Edge with ID " + edge.id() + " that has been created in this transaction, but the store contains another edge with the same ID that has different neighboring vertices!");
                        }
                    }
                    this.copyProperties(edge, storeEdge);
                    break;
                }
                case REMOVED: {
                    Edge storeEdge = Iterators.getOnlyElement(currentStateGraph.edges(edge.id()), null);
                    if (storeEdge != null) {
                        storeEdge.remove();
                    }
                    break;
                }
                case OBSOLETE: {
                    // ignore
                    break;
                }
                case EDGE_CHANGED: {
                    // can't happen for edges
                    throw new IllegalStateException("Detected Edge in lifecycle status EDGE_CHANGED!");
                }
                case PERSISTED: {
                    // ignore, edge is already in sync with store
                    break;
                }
                case PROPERTY_CHANGED: {
                    Edge storeEdge = Iterators.getOnlyElement(currentStateGraph.edges(edge.id()), null);
                    if (storeEdge != null) {
                        Set<String> propertyKeys = Sets.newHashSet();
                        propertyKeys.addAll(edge.keys());
                        propertyKeys.addAll(storeEdge.keys());
                        for (String propertyKey : propertyKeys) {
                            PropertyStatus propertyStatus = edge.getPropertyStatus(propertyKey);
                            switch (propertyStatus) {
                                case NEW:
                                    // FALL THROUGH
                                case MODIFIED:
                                    Property<?> property = edge.property(propertyKey);
                                    storeEdge.property(propertyKey, property.value());
                                    break;
                                case PERSISTED:
                                    // ignore, property is untouched in transaction
                                    break;
                                case REMOVED:
                                    // remove in store as well
                                    storeEdge.property(propertyKey).remove();
                                    break;
                                case UNKNOWN:
                                    // ignore, property is unknown in transaction
                                    break;
                                default:
                                    throw new UnknownEnumLiteralException(propertyStatus);
                            }
                        }
                    }
                    break;
                }
                default:
                    throw new UnknownEnumLiteralException(status);
            }
        }
    }


    private void mergeGraphVariableChangesInto(final ChronoGraph currentStateGraph) {
        Set<String> modifiedVariables = this.getContext().getModifiedVariables();
        Graph.Variables variables = currentStateGraph.variables();
        for (String variable : modifiedVariables) {
            Object value = this.getContext().getModifiedVariableValue(variable);
            if (value == null) {
                variables.remove(variable);
            } else {
                variables.set(variable, value);
            }
        }
    }

    private <E extends Element> void copyProperties(final E sourceElement, final E targetElement) {
        Iterator<? extends Property<Object>> propertyIterator = sourceElement.properties();
        while (propertyIterator.hasNext()) {
            Property<?> prop = propertyIterator.next();
            Property<?> storeProp = targetElement.property(prop.key(), prop.value());
            if (prop instanceof VertexProperty) {
                VertexProperty<?> vProp = (VertexProperty<?>) prop;
                this.copyMetaProperties(vProp, (VertexProperty<?>) storeProp);
            }
        }
    }

    private void copyMetaProperties(final VertexProperty<?> source, final VertexProperty<?> target) {
        Iterator<Property<Object>> metaPropertyIterator = source.properties();
        while (metaPropertyIterator.hasNext()) {
            Property<?> metaProp = metaPropertyIterator.next();
            target.property(metaProp.key(), metaProp.value());
        }
    }

    private void mapModifiedVerticesToChronoDB() {
        // get the backing transaction
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        // read the set of modified vertices
        Set<ChronoVertexImpl> modifiedVertices = this.context.getModifiedVertices();
        // write each vertex into a key-value pair in the transaction
        for (ChronoVertexImpl vertex : modifiedVertices) {
            String vertexId = vertex.id();
            ElementLifecycleStatus vertexStatus = vertex.getStatus();
            switch (vertexStatus) {
                case NEW:
                    tx.put(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, vertex.toRecord());
                    break;
                case OBSOLETE:
                    // obsolete graph elements are not committed to the store,
                    // they have been created AND removed in the same transaction
                    break;
                case EDGE_CHANGED:
                    tx.put(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, vertex.toRecord(), PutOption.NO_INDEX);
                    break;
                case PERSISTED:
                    // this case should actually be unreachable because persisted elements are clean and not dirty
                    throw new IllegalStateException(
                            "Unreachable code reached: PERSISTED vertex '" + vertexId + "' is listed as dirty!");
                case PROPERTY_CHANGED:
                    tx.put(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, vertex.toRecord());
                    break;
                case REMOVED:
                    tx.remove(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId);
                    break;
                default:
                    throw new UnknownEnumLiteralException(vertexStatus);
            }
        }
    }

    private void mapModifiedEdgesToChronoDB() {
        // get the backing transaction
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        // read the set of modified edges
        Set<ChronoEdgeImpl> modifiedEdges = this.context.getModifiedEdges();
        // write each edge into a key-value pair in the transaction
        for (ChronoEdgeImpl edge : modifiedEdges) {
            String edgeId = edge.id();
            ElementLifecycleStatus edgeStatus = edge.getStatus();
            switch (edgeStatus) {
                case NEW:
                    if (CCC.TRACE_ENABLED) {
                        ChronoLogger.logTrace("[COMMIT]: Committing Edge '" + edgeId + "' in status NEW");
                    }
                    tx.put(ChronoGraphConstants.KEYSPACE_EDGE, edgeId, edge.toRecord());
                    break;
                case EDGE_CHANGED:
                    throw new IllegalStateException(
                            "Unreachable code reached: Detected edge '" + edgeId + "' in state EDGE_CHANGED!");
                case OBSOLETE:
                    if (CCC.TRACE_ENABLED) {
                        ChronoLogger.logTrace("[COMMIT]: Ignoring Edge '" + edgeId + "' in status OBSOLETE");
                    }
                    // obsolete graph elements are not committed to the store,
                    // they have been created AND removed in the same transaction
                    break;
                case PERSISTED:
                    throw new IllegalStateException(
                            "Unreachable code reached: PERSISTED edge '" + edgeId + "' is listed as dirty!");
                case PROPERTY_CHANGED:
                    if (CCC.TRACE_ENABLED) {
                        ChronoLogger.logTrace("[COMMIT]: Committing Edge '" + edgeId + "' in status PROPERTY_CHANGED");
                    }
                    tx.put(ChronoGraphConstants.KEYSPACE_EDGE, edgeId, edge.toRecord());
                    break;
                case REMOVED:
                    if (CCC.TRACE_ENABLED) {
                        ChronoLogger.logTrace("[COMMIT]: Removing Edge '" + edgeId + "' in status REMOVED");
                    }
                    tx.remove(ChronoGraphConstants.KEYSPACE_EDGE, edgeId);
                    break;
                default:
                    break;
            }
        }
    }

    private void mapModifiedGraphVariablesToChronoDB() {
        // get the backing transaction
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        // write the modifications of graph variables into the transaction context
        for (String variableName : this.context.getModifiedVariables()) {
            if (this.context.isVariableRemoved(variableName)) {
                tx.remove(ChronoGraphConstants.KEYSPACE_VARIABLES, variableName);
            } else {
                tx.put(ChronoGraphConstants.KEYSPACE_VARIABLES, variableName,
                        this.context.getModifiedVariableValue(variableName));
            }
        }
    }

    // =====================================================================================================================
    // DEBUG LOGGING
    // =====================================================================================================================

    private void logAddVertex(final String vertexId, final boolean isUserProvided) {
        if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
            // log level is higher than trace, no need to prepare the message
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[GRAPH MODIFICATION] Adding Vertex with ");
        if (isUserProvided) {
            messageBuilder.append("user-provided ");
        } else {
            messageBuilder.append("auto-generated ");
        }
        messageBuilder.append("ID '");
        messageBuilder.append(vertexId);
        messageBuilder.append("' to graph.");
        ChronoLogger.logTrace(messageBuilder.toString());
    }

}
