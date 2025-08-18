package org.chronos.chronograph.internal.impl.transaction;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.EntryStatus.Deleted;
import org.chronos.chronodb.api.EntryStatus.Exists;
import org.chronos.chronodb.api.EntryStatus.NotFound;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.impl.engines.base.StandardChronoDBTransaction;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.exceptions.ChronoGraphCommitConflictException;
import org.chronos.chronograph.api.exceptions.ChronoGraphSchemaViolationException;
import org.chronos.chronograph.api.exceptions.GraphInvariantViolationException;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.schema.SchemaValidationResult;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoGraphVariables;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus;
import org.chronos.chronograph.api.structure.PropertyStatus;
import org.chronos.chronograph.api.structure.record.IEdgeRecord;
import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;
import org.chronos.chronograph.api.structure.record.IVertexRecord;
import org.chronos.chronograph.api.transaction.AllEdgesIterationHandler;
import org.chronos.chronograph.api.transaction.AllVerticesIterationHandler;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.EdgeStatus;
import org.chronos.chronograph.api.transaction.GraphTransactionContext;
import org.chronos.chronograph.api.transaction.VertexStatus;
import org.chronos.chronograph.api.transaction.trigger.CancelCommitException;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostPersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPreCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPrePersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.PostCommitTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PostPersistTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PreCommitTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PrePersistTriggerContext;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.internal.api.structure.ChronoElementInternal;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoGraphVariablesImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleEvent;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoEdgeProxy;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoVertexProxy;
import org.chronos.chronograph.internal.impl.structure.graph.readonly.ReadOnlyChronoEdge;
import org.chronos.chronograph.internal.impl.structure.graph.readonly.ReadOnlyChronoVertex;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;
import org.chronos.chronograph.internal.impl.transaction.trigger.PostTriggerContextImpl;
import org.chronos.chronograph.internal.impl.transaction.trigger.PreTriggerContextImpl;
import org.chronos.chronograph.internal.impl.util.ChronoGraphLoggingUtil;
import org.chronos.chronograph.internal.impl.util.ChronoId;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;
import static org.chronos.common.logging.ChronosLogMarker.*;

public class StandardChronoGraphTransaction implements ChronoGraphTransaction, ChronoGraphTransactionInternal {

    private static final Logger log = LoggerFactory.getLogger(StandardChronoGraphTransaction.class);

    private final String transactionId;
    private final ChronoGraphInternal graph;
    private ChronoDBTransaction backendTransaction;
    private GraphTransactionContextInternal context;
    private final ChronoGraphQueryProcessor queryProcessor;

    private long rollbackCount;

    public StandardChronoGraphTransaction(final ChronoGraphInternal graph,
                                          final ChronoDBTransaction backendTransaction) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        checkNotNull(backendTransaction, "Precondition violation - argument 'backendTransaction' must not be NULL!");
        this.transactionId = UUID.randomUUID().toString();
        this.graph = graph;
        this.backendTransaction = backendTransaction;
        this.context = new GraphTransactionContextImpl();
        this.rollbackCount = 0L;
        this.queryProcessor = new ChronoGraphQueryProcessor(this);
    }

    // =====================================================================================================================
    // METADATA
    // =====================================================================================================================

    @Override
    public ChronoDBTransaction getBackingDBTransaction() {
        this.assertIsOpen();
        return this.backendTransaction;
    }

    @Override
    public GraphTransactionContext getContext() {
        this.assertIsOpen();
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

    protected ChronoGraphInternal getGraphInternal() {
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
        // by default, the tx is open as long as the graph is open.
        return !this.graph.isClosed();
    }

    // =====================================================================================================================
    // COMMIT & ROLLBACK
    // =====================================================================================================================

    @Override
    public long commit() {
        return this.commit(null);
    }

    @Override
    public long commit(final Object metadata) {
        this.assertIsOpen();
        boolean performanceLoggingActive = this.graph.getBackingDB().getConfiguration().isCommitPerformanceLoggingActive();
        long timeBeforeGraphCommit = System.currentTimeMillis();
        long commitTimestamp = -1;
        long beforePreCommitTriggers = System.currentTimeMillis();
        this.firePreCommitTriggers(metadata);
        String perfLogPrefix = "[PERF ChronoGraph] Graph Commit (" + this.getBranchName() + "@" + this.getTimestamp() + ")";
        if (performanceLoggingActive) {
            log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Pre-Commit Triggers: " + (System.currentTimeMillis() - beforePreCommitTriggers) + "ms.");
        }
        long timeBeforeLockAcquisition = System.currentTimeMillis();
        try (AutoLock lock = this.graph.commitLock()) {
            if (performanceLoggingActive) {
                log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Graph Commit Lock Acquisition: " + (System.currentTimeMillis() - timeBeforeLockAcquisition) + "ms.");
            }
            // only try to merge if not in incremental commit mode
            if (this.getBackingDBTransaction().isInIncrementalCommitMode() == false) {
                long timeBeforeMerge = System.currentTimeMillis();
                this.performGraphLevelMergeWithStoreState();
                if (performanceLoggingActive) {
                    log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Graph-Level Merge With Store: " + (System.currentTimeMillis() - timeBeforeMerge) + "ms.");
                }
            }
            long timeBeforePrePersistTriggers = System.currentTimeMillis();
            this.firePrePersistTriggers(metadata);
            if (performanceLoggingActive) {
                log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Pre-Persist Triggers: " + (System.currentTimeMillis() - timeBeforePrePersistTriggers) + "ms.");
            }
            ChronoGraphConfiguration config = this.getGraph().getChronoGraphConfiguration();
            if (config.isGraphInvariantCheckActive()) {
                // validate the graph invariant (each edge points to two existing verticies)
                long timeBeforeGraphInvariantCheck = System.currentTimeMillis();
                this.validateGraphInvariant();
                if (performanceLoggingActive) {
                    log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Graph Invariant Check: " + (System.currentTimeMillis() - timeBeforeGraphInvariantCheck) + "ms.");
                }
            }
            // perform the schema validation (if any)
            long timeBeforeSchemaValidation = System.currentTimeMillis();
            SchemaValidationResult schemaValidationResult = this.performGraphSchemaValidation();
            if (performanceLoggingActive) {
                log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Schema Validation Check: " + (System.currentTimeMillis() - timeBeforeSchemaValidation) + "ms.");
            }
            if (schemaValidationResult.isFailure()) {
                this.rollback();
                throw new ChronoGraphSchemaViolationException(schemaValidationResult.generateErrorMessage());
            }
            // merge not required, commit this transaction
            long timeBeforeVertexMap = System.currentTimeMillis();
            this.mapModifiedVerticesToChronoDB();
            if (performanceLoggingActive) {
                log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Mapping Vertices to Key-Value pairs: " + (System.currentTimeMillis() - timeBeforeVertexMap) + "ms.");
            }
            long timeBeforeEdgesMap = System.currentTimeMillis();
            this.mapModifiedEdgesToChronoDB();
            if (performanceLoggingActive) {
                log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Mapping Edges to Key-Value pairs: " + (System.currentTimeMillis() - timeBeforeEdgesMap) + "ms.");
            }
            long timeBeforeVariablesMap = System.currentTimeMillis();
            this.mapModifiedGraphVariablesToChronoDB();
            if (performanceLoggingActive) {
                log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Mapping Graph Variables to Key-Value pairs: " + (System.currentTimeMillis() - timeBeforeVariablesMap) + "ms.");
            }
            if (log.isTraceEnabled(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
                String header = ChronoGraphLoggingUtil.createLogHeader(this);
                log.trace(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, header + "Committing Transaction.");
            }
            // commit the transaction
            long timeBeforeChronoDBCommit = System.currentTimeMillis();
            commitTimestamp = this.getBackingDBTransaction().commit(metadata);
            if (performanceLoggingActive) {
                log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> ChronoDB commit: " + (System.currentTimeMillis() - timeBeforeChronoDBCommit) + "ms. Commit Timestamp: " + commitTimestamp);
            }
            // preserve some information for the post-persist triggers (if necessary)
            if (commitTimestamp >= 0) {
                // only fire post-persist triggers if the transaction actually changed something
                long timeBeforePostPersistTriggers = System.currentTimeMillis();
                this.firePostPersistTriggers(commitTimestamp, metadata);
                if (performanceLoggingActive) {
                    log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Post Persist Triggers: " + (System.currentTimeMillis() - timeBeforePostPersistTriggers) + "ms.");
                }
            }
        }
        if (commitTimestamp >= 0) {
            // only fire post-commit triggers if the transaction actually changed something
            long timeBeforePostCommitTriggers = System.currentTimeMillis();
            this.firePostCommitTriggers(commitTimestamp, metadata);
            if (performanceLoggingActive) {
                log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + " -> Post Commit Triggers: " + (System.currentTimeMillis() - timeBeforePostCommitTriggers) + "ms.");
            }
        }
        // clear the transaction context
        this.context = new GraphTransactionContextImpl();
        if (performanceLoggingActive) {
            log.info(CHRONOS_LOG_MARKER__PERFORMANCE, perfLogPrefix + ": " + (System.currentTimeMillis() - timeBeforeGraphCommit) + "ms.");
        }
        return commitTimestamp;
    }


    @Override
    public void commitIncremental() {
        this.assertIsOpen();
        try (AutoLock lock = this.graph.commitLock()) {
            if (this.getBackingDBTransaction().isInIncrementalCommitMode() == false) {
                // we're not yet in incremental commit mode, assert that the timestamp is the latest
                long now;
                try (ChronoGraph currentStateGraph = this.graph.tx().createThreadedTx(this.getBranchName())) {
                    now = currentStateGraph.getNow(this.getBranchName());
                    currentStateGraph.tx().rollback();
                }
                if (now != this.getTimestamp()) {
                    throw new IllegalStateException("Cannot perform incremental commit: a concurrent transaction has performed a commit since this transaction was opened!");
                }
            }
            ChronoGraphConfiguration config = this.getGraph().getChronoGraphConfiguration();
            if (config.isGraphInvariantCheckActive()) {
                // validate the graph invariant (each edge points to two existing verticies)
                this.validateGraphInvariant();
            }
            // perform the schema validation (if any)
            SchemaValidationResult schemaValidationResult = this.performGraphSchemaValidation();
            if (schemaValidationResult.isFailure()) {
                this.rollback();
                throw new ChronoGraphSchemaViolationException(schemaValidationResult.generateErrorMessage());
            }
            this.mapModifiedVerticesToChronoDB();
            this.mapModifiedEdgesToChronoDB();
            this.mapModifiedGraphVariablesToChronoDB();
            if (log.isTraceEnabled(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
                String header = ChronoGraphLoggingUtil.createLogHeader(this);
                log.trace(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, header + "Performing Incremental Commit.");
            }
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
        if (log.isTraceEnabled(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            String header = ChronoGraphLoggingUtil.createLogHeader(this);
            log.trace(CHRONOS_LOG_MARKER__PERFORMANCE, header + "Rolling back transaction.");
        }
        this.getBackingDBTransaction().rollback();
    }

    // =====================================================================================================================
    // QUERY METHODS
    // =====================================================================================================================

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        this.assertIsOpen();
        if (vertexIds == null || vertexIds.length <= 0) {
            // query all vertices... this is bad.
            AllVerticesIterationHandler handler = this.getGraph().getChronoGraphConfiguration().getAllVerticesIterationHandler();
            if (handler != null) {
                handler.onAllVerticesIteration();
            }
            Set<ChronoGraphIndex> dirtyIndices = this.getGraph().getIndexManagerOnBranch(this.getBranchName()).getDirtyIndicesAtTimestamp(this.getTimestamp());
            if (dirtyIndices.isEmpty()) {
                log.warn("Query requires iterating over all vertices."
                    + " For better performance, use 'has(...)' clauses in your gremlin that can utilize indices.");
            } else {
                log.warn(
                    "Query requires iterating over all vertices which results in reduced performance. " +
                        "This may be due to the following indices being dirty: " + dirtyIndices
                );
            }
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
        throw new IllegalArgumentException("The given 'vertexIds' arguments must either be all of type String or all of type Vertex!");
    }

    @Override
    public Iterator<Vertex> getAllVerticesIterator() {
        this.assertIsOpen();
        return this.queryProcessor.getAllVerticesIterator();
    }

    @Override
    public Iterator<Vertex> getVerticesIterator(final Iterable<String> chronoVertexIds,
                                                final ElementLoadMode loadMode) {
        checkNotNull(chronoVertexIds, "Precondition violation - argument 'chronoVertexIds' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        this.assertIsOpen();
        return this.queryProcessor.getVerticesIterator(chronoVertexIds, loadMode);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (edgeIds == null || edgeIds.length <= 0) {
            // query all edges... this is bad.
            AllEdgesIterationHandler handler = this.getGraph().getChronoGraphConfiguration().getAllEdgesIterationHandler();
            if (handler != null) {
                handler.onAllEdgesIteration();
            }
            Set<ChronoGraphIndex> dirtyIndices = this.getGraph().getIndexManagerOnBranch(this.getBranchName()).getDirtyIndicesAtTimestamp(this.getTimestamp());
            if (dirtyIndices.isEmpty()) {
                log.warn("Query requires iterating over all edges."
                    + " For better performance, use 'has(...)' clauses in your gremlin that can utilize indices.");
            } else {
                log.warn(
                    "Query requires iterating over all edges which results in reduced performance. " +
                        "This may be due to the following indices being dirty: " + dirtyIndices
                );
            }
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
        throw new IllegalArgumentException("The given 'vertexIds' arguments must either be all of type String or all of type Edge!");
    }

    @Override
    public Iterator<Edge> getAllEdgesIterator() {
        return this.queryProcessor.getAllEdgesIterator();
    }

    @Override
    public Iterator<Edge> getEdgesIterator(final Iterable<String> edgeIds, final ElementLoadMode loadMode) {
        checkNotNull(edgeIds, "Precondition violation - argument 'edgeIds' must not be NULL!");
        return this.queryProcessor.getEdgesIterator(edgeIds, loadMode);
    }

    @Override
    public VertexStatus vertexStatus(final String vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        var entryStatus = this.backendTransaction.getStatus(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId);
        return switch (entryStatus) {
            case Deleted deleted -> new VertexStatus.Deleted(deleted.lastModificationTimestamp());
            case Exists exists -> new VertexStatus.Exists(
                exists.lastModificationTimestamp(),
                this.loadVertex(vertexId, ElementLoadMode.LAZY)
            );
            case NotFound ignored -> VertexStatus.NotFound.INSTANCE;
        };
    }

    @Override
    public EdgeStatus edgeStatus(final String edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'vertexId' must not be NULL!");
        var entryStatus = this.backendTransaction.getStatus(ChronoGraphConstants.KEYSPACE_EDGE, edgeId);
        return switch (entryStatus) {
            case Deleted deleted -> new EdgeStatus.Deleted(deleted.lastModificationTimestamp());
            case Exists exists -> new EdgeStatus.Exists(
                exists.lastModificationTimestamp(),
                this.loadEdge(edgeId, ElementLoadMode.LAZY)
            );
            case NotFound ignored -> EdgeStatus.NotFound.INSTANCE;
        };
    }

    // =====================================================================================================================
    // TEMPORAL QUERY METHODS
    // =====================================================================================================================


    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkArgument(lowerBound >= 0, "Precondition violation - argument 'lowerBound' must not be negative!");
        checkArgument(upperBound >= 0, "Precondition violation - argument 'upperBound' must not be negative!");
        checkArgument(lowerBound <= upperBound, "Precondition violation - argument 'lowerBound' must be less than or equal to argument 'upperBound'!");
        checkNotNull(order);
        if (vertexId instanceof Vertex) {
            return this.getVertexHistory(((Vertex) vertexId).id(), lowerBound, upperBound, order);
        }
        if (vertexId instanceof String) {
            return this.getVertexHistory((String) vertexId, lowerBound, upperBound, order);
        }
        throw new IllegalArgumentException("The given object is no valid vertex id: " + vertexId);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object edgeId, final long lowerBound, final long upperBound, final Order order) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkArgument(lowerBound >= 0, "Precondition violation - argument 'lowerBound' must not be negative!");
        checkArgument(upperBound >= 0, "Precondition violation - argument 'upperBound' must not be negative!");
        checkArgument(lowerBound <= upperBound, "Precondition violation - argument 'lowerBound' must be less than or equal to argument 'upperBound'!");
        checkNotNull(order);
        if (edgeId instanceof Edge) {
            return this.getEdgeHistory((Edge) edgeId, lowerBound, upperBound, order);
        }
        if (edgeId instanceof String) {
            return this.getEdgeHistory((String) edgeId, lowerBound, upperBound, order);
        }
        throw new IllegalArgumentException("The given object is no valid edge id: " + edgeId);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Vertex vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        return this.getLastModificationTimestampOfVertex(vertex.id());
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Object vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        if (vertexId instanceof Vertex) {
            return this.getLastModificationTimestampOfVertex(((Vertex) vertexId).id());
        } else if (vertexId instanceof String) {
            return this.getBackingDBTransaction().getLastModificationTimestamp(ChronoGraphConstants.KEYSPACE_VERTEX, (String) vertexId);
        }
        throw new IllegalArgumentException("The given object is no valid vertex id: " + vertexId);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        return this.getLastModificationTimestampOfEdge(edge.id());
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Object edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        if (edgeId instanceof Edge) {
            return this.getLastModificationTimestampOfEdge(((Edge) edgeId).id());
        } else if (edgeId instanceof String) {
            return this.getBackingDBTransaction().getLastModificationTimestamp(ChronoGraphConstants.KEYSPACE_EDGE, (String) edgeId);
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
        ChronoVertex removedVertex = null;
        if (userProvidedId) {
            // assert that we don't already have a graph element with this ID in our transaction cache
            ChronoVertex modifiedVertex = this.getContext().getModifiedVertex(vertexId);
            if (modifiedVertex != null) {
                // re-creating a vertex that has been removed is ok
                if (!modifiedVertex.isRemoved()) {
                    throw Graph.Exceptions.vertexWithIdAlreadyExists(vertexId);
                } else {
                    // the vertex already existed in our transaction context, but was removed. Now
                    // it is being recreated.
                    removedVertex = modifiedVertex;
                }
            } else {
                // assert that we don't already have a graph element with this ID in our persistence
                if (this.getGraph().getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled()) {
                    ChronoDBTransaction backingTx = this.getBackingDBTransaction();
                    boolean vertexIdAlreadyExists = backingTx.exists(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId);
                    if (vertexIdAlreadyExists) {
                        throw Graph.Exceptions.vertexWithIdAlreadyExists(vertexId);
                    }
                }
            }
        }
        this.logAddVertex(vertexId, userProvidedId);
        ChronoVertexImpl vertex = new ChronoVertexImpl(vertexId, this.graph, this, label);
        if (removedVertex != null) {
            // we already have this vertex in our transaction context
            if (removedVertex.getStatus() == ElementLifecycleStatus.OBSOLETE) {
                vertex.updateLifecycleStatus(ElementLifecycleEvent.RECREATED_FROM_OBSOLETE);
            } else if (removedVertex.getStatus() == ElementLifecycleStatus.REMOVED) {
                vertex.updateLifecycleStatus(ElementLifecycleEvent.RECREATED_FROM_REMOVED);
            } else {
                throw new IllegalStateException("Vertex '" + vertex.id() + "' is removed, but is neither " + ElementLifecycleStatus.REMOVED + " nor " + ElementLifecycleStatus.OBSOLETE + "!");
            }
        } else {
            vertex.updateLifecycleStatus(ElementLifecycleEvent.CREATED);
        }
        ElementHelper.attachProperties(vertex, keyValues);
        this.context.registerLoadedVertex(vertex);
        return this.context.getOrCreateVertexProxy(vertex);
    }

    @Override
    public ChronoEdge addEdge(final ChronoVertex outVertex, final ChronoVertex inVertex, final String id,
                              final boolean isUserProvidedId, final String label, final Object... keyValues) {
        ChronoEdge removedEdge = null;
        if (isUserProvidedId) {
            // assert that we don't already have a graph element with this ID in our transaction cache
            ChronoEdge modifiedEdge = this.getContext().getModifiedEdge(id);
            if (modifiedEdge != null) {
                if (modifiedEdge.isRemoved() == false) {
                    throw Graph.Exceptions.edgeWithIdAlreadyExists(id);
                } else {
                    // the edge was removed in the transaction context and is now created again
                    removedEdge = modifiedEdge;
                }
            } else {
                // edge is "clean"
                if (this.getGraph().getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled()) {
                    // assert that we don't already have a graph element with this ID in our persistence
                    ChronoDBTransaction backingTx = this.getBackingDBTransaction();
                    boolean edgeIdAlreadyExists = backingTx.exists(ChronoGraphConstants.KEYSPACE_EDGE, id);
                    if (edgeIdAlreadyExists) {
                        throw Graph.Exceptions.edgeWithIdAlreadyExists(id);
                    }
                }
            }
        }
        // create the edge
        ChronoVertexImpl outV = ChronoProxyUtil.resolveVertexProxy(outVertex);
        ChronoVertexImpl inV = ChronoProxyUtil.resolveVertexProxy(inVertex);
        ChronoEdgeImpl edge = ChronoEdgeImpl.create(id, outV, label, inV);
        // if we have a previous element...
        if (removedEdge != null) {
            // ... then we are dealing with a recreation
            if (removedEdge.getStatus() == ElementLifecycleStatus.OBSOLETE) {
                edge.updateLifecycleStatus(ElementLifecycleEvent.RECREATED_FROM_OBSOLETE);
            } else if (removedEdge.getStatus() == ElementLifecycleStatus.REMOVED) {
                edge.updateLifecycleStatus(ElementLifecycleEvent.RECREATED_FROM_REMOVED);
            } else {
                throw new IllegalStateException("Edge '" + edge.id() + "' is removed, but is neither " + ElementLifecycleStatus.REMOVED + " nor " + ElementLifecycleStatus.OBSOLETE + "!");
            }
        }

        // set the properties (if any)
        ElementHelper.attachProperties(edge, keyValues);
        this.context.registerLoadedEdge(edge);
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

    public ChronoVertex loadVertex(final String id, final ElementLoadMode loadMode) {
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        // first, try to find the vertex in our 'modified vertices' cache
        ChronoVertexImpl modifiedVertex = (ChronoVertexImpl) this.context.getModifiedVertex(id);
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
                IVertexRecord record = tx.get(ChronoGraphConstants.KEYSPACE_VERTEX, id.toString());
                // load the vertex from the database
                if (record == null) {
                    return null;
                }
                vertex = new ChronoVertexImpl(this.graph, this, record);
                // register the loaded instance
                this.context.registerLoadedVertex(vertex);
                return vertex;
            case LAZY:
                // we can trust that there actually IS a vertex in the database for the given ID,
                // but we want to load it lazily when a vertex property is first accessed.
                ChronoVertexProxy proxy = new ChronoVertexProxy(this.graph, id);
                this.context.registerVertexProxyInCache(proxy);
                return proxy;
            default:
                throw new UnknownEnumLiteralException(loadMode);
        }
    }

    public ChronoEdge loadEdge(final String id, ElementLoadMode loadMode) {
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        // first, try to find the edge in our 'modified edges' cache
        ChronoEdgeImpl modifiedEdge = (ChronoEdgeImpl) this.context.getModifiedEdge(id);
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
        switch (loadMode) {
            case LAZY:
                // we know we can trust that there actually IS an edge with
                // the given ID without checking in the store, so we only
                // create a proxy here which will lazy-load the edge when required.
                // This is important because of queries such as:
                //
                // graph.traversal().E().has(p, X).count()
                //
                // ... where "p" is an indexed property. If we did not load lazily,
                // then the "count" would require to load all individual edges (!!)
                // from disk, which is clearly unnecessary.
                ChronoEdgeProxy proxy = new ChronoEdgeProxy(this.graph, id);
                this.context.registerEdgeProxyInCache(proxy);
                return proxy;
            case EAGER:
                // load the edge from the database
                ChronoDBTransaction tx = this.getBackingDBTransaction();
                IEdgeRecord record = tx.get(ChronoGraphConstants.KEYSPACE_EDGE, id);
                if (record == null) {
                    return null;
                }
                ChronoEdgeImpl edge = ChronoEdgeImpl.create(this.graph, this, record);
                // register the loaded edge
                this.context.registerLoadedEdge(edge);
                return edge;
            default:
                throw new UnknownEnumLiteralException(loadMode);
        }
    }

    @Override
    public ChronoEdge loadOutgoingEdgeFromEdgeTargetRecord(final ChronoVertexImpl sourceVertex, final String label,
                                                           final IEdgeTargetRecord record) {
        checkNotNull(sourceVertex, "Precondition violation - argument 'sourceVertex' must not be NULL!");
        checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
        checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
        String id = record.getEdgeId();
        // first, try to find the edge in our 'modified edges' cache
        ChronoEdge modifiedEdge = this.context.getModifiedEdge(id);
        if (modifiedEdge != null) {
            // the given vertex is in our 'modified edges' cache; reuse it
            return modifiedEdge;
        }
        // then, try to find it in our 'already loaded' cache
        ChronoEdge loadedEdge = this.context.getLoadedEdgeForId(id);
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
    public IVertexRecord loadVertexRecord(final String recordId) {
        checkNotNull(recordId, "Precondition violation - argument 'recordId' must not be NULL!");
        Object value = this.backendTransaction.get(ChronoGraphConstants.KEYSPACE_VERTEX, recordId);
        if (value == null) {
            throw new IllegalStateException("Received request (branch: '" + this.getBranchName() + "', timestamp: " + this.getTimestamp() + ") to fetch Vertex Record with ID '" + recordId + "', but no record was found!");
        }
        if (value instanceof IVertexRecord) {
            return (IVertexRecord) value;
        } else {
            throw new IllegalStateException("Received request to fetch Vertex Record with ID '" + recordId + "', but the database returned an object of incompatible type '" + value.getClass().getName() + "' instead! Branch: " + this.getBranchName() + ", timestamp: " + this.getTimestamp());
        }
    }

    @Override
    public IEdgeRecord loadEdgeRecord(final String recordId) {
        checkNotNull(recordId, "Precondition violation - argument 'recordId' must not be NULL!");
        Object value = this.backendTransaction.get(ChronoGraphConstants.KEYSPACE_EDGE, recordId);
        if (value == null) {
            throw new IllegalStateException("Received request (branch: '" + this.getBranchName() + "', timestamp: " + this.getTimestamp() + ") to fetch Edge Record with ID '" + recordId + "', but no record was found!");
        }
        if (value instanceof IEdgeRecord) {
            return (IEdgeRecord) value;
        } else {
            throw new IllegalStateException("Received request (branch: '" + this.getBranchName() + "', timestamp: " + this.getTimestamp() + ") to fetch Edge Record with ID '" + recordId + "', but the database returned an object of incompatible type '" + value.getClass().getName() + "'!");
        }
    }

    @Override
    public ChronoEdge loadIncomingEdgeFromEdgeTargetRecord(final ChronoVertexImpl targetVertex, final String label,
                                                           final IEdgeTargetRecord record) {
        checkNotNull(targetVertex, "Precondition violation - argument 'targetVertex' must not be NULL!");
        checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
        checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
        String id = record.getEdgeId();
        // first, try to find the edge in our 'modified edges' cache
        ChronoEdge modifiedEdge = this.context.getModifiedEdge(id);
        if (modifiedEdge != null) {
            // the given vertex is in our 'modified edges' cache; reuse it
            return modifiedEdge;
        }
        // then, try to find it in our 'already loaded' cache
        ChronoEdge loadedEdge = this.context.getLoadedEdgeForId(id);
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

    private Iterator<Long> getVertexHistory(final String vertexId, long lowerBound, long upperBound, Order order) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        return tx.history(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, lowerBound, upperBound, order);
    }

    private Iterator<Long> getEdgeHistory(final String edgeId, long lowerBound, long upperBound, Order order) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        return tx.history(ChronoGraphConstants.KEYSPACE_EDGE, edgeId, lowerBound, upperBound, order);
    }

    private void performGraphLevelMergeWithStoreState() {
        GraphTransactionContext oldContext = this.context;
        // reset the transaction
        this.context = new GraphTransactionContextImpl();
        ((StandardChronoDBTransaction) this.backendTransaction).cancelAndResetToHead();
        this.mergeVertexChangesFrom(oldContext);
        this.mergeEdgeChangesFrom(oldContext);
        this.mergeGraphVariableChangesFrom(oldContext);
    }


    private void mergeVertexChangesFrom(final GraphTransactionContext oldContext) {
        Set<ChronoVertex> verticesToSynchronize = Sets.newHashSet(oldContext.getModifiedVertices());
        for (ChronoVertex vertex : verticesToSynchronize) {
            ElementLifecycleStatus status = vertex.getStatus();
            switch (status) {
                case REMOVED: {
                    Vertex storeVertex = Iterators.getOnlyElement(this.graph.vertices(vertex.id()), null);
                    if (storeVertex != null) {
                        // delete the vertex in the store
                        storeVertex.remove();
                    }
                    break;
                }
                case OBSOLETE:
                case PERSISTED: {
                    // ignore
                    break;
                }
                case NEW: {
                    // the vertex is new in the current transaction
                    Vertex storeVertex = Iterators.getOnlyElement(this.graph.vertices(vertex.id()), null);
                    if (storeVertex == null) {
                        // create the vertex
                        storeVertex = this.graph.addVertex(T.id, vertex.id(), T.label, vertex.label());
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
                    Vertex storeVertex = Iterators.getOnlyElement(this.graph.vertices(vertex.id()), null);
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
                                    VertexProperty<?> txProperty = vertex.property(propertyKey);
                                    VertexProperty<?> storeProperty = storeVertex.property(propertyKey);
                                    // are the properties exactly the same?
                                    if (!arePropertiesTheSame(txProperty, storeProperty)) {
                                        // the properties are different, apply tx changes to store state
                                        storeVertex.property(propertyKey, txProperty.value());
                                    }
                                    if (!areMetaPropertiesTheSame(txProperty, storeProperty)) {
                                        VertexProperty<?> targetProperty = storeVertex.property(propertyKey);
                                        Set<String> allKeys = Sets.union(txProperty.keys(), targetProperty.keys());
                                        for (String metaKey : allKeys) {
                                            Property<Object> txMetaProperty = txProperty.property(metaKey);
                                            if (!txMetaProperty.isPresent()) {
                                                storeProperty.property(metaKey).remove();
                                            } else {
                                                Property<Object> storeMetaProperty = storeProperty.property(metaKey);
                                                if (storeMetaProperty.isPresent()) {
                                                    Object txMetaValue = txMetaProperty.value();
                                                    Object storeMetaValue = storeMetaProperty.value();
                                                    if (!Objects.equal(storeMetaValue, txMetaValue)) {
                                                        storeProperty.property(metaKey, txMetaValue);
                                                    }
                                                } else {
                                                    storeProperty.property(metaKey, txProperty.value(metaKey));
                                                }
                                            }
                                        }
                                    }
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
                default:
                    throw new UnknownEnumLiteralException(status);
            }

        }
    }

    private boolean arePropertiesTheSame(Property<?> left, Property<?> right) {
        boolean leftExists = left.isPresent();
        boolean rightExists = right.isPresent();
        if (!leftExists && !rightExists) {
            // special case: neither of the two properties exists, therefore they are the same.
            return true;
        }
        if (leftExists != rightExists) {
            // either one of them does not exist, therefore they are not the same.
            return false;
        }
        // in the remaining case, both properties do exist.
        if (!Objects.equal(left.value(), right.value())) {
            // values are different
            return false;
        }
        // no changes detected -> the properties are the same.
        return true;
    }

    private boolean areMetaPropertiesTheSame(VertexProperty<?> left, VertexProperty<?> right) {
        Set<String> leftKeys = left.keys();
        Set<String> rightKeys = right.keys();
        if (leftKeys.size() != rightKeys.size()) {
            // meta-properties are different -> properties are different.
            return false;
        }
        Set<String> allKeys = Sets.union(leftKeys, rightKeys);
        for (String metaPropertyKey : allKeys) {
            Property<?> leftMetaProp = left.property(metaPropertyKey);
            Property<?> rightMetaProp = right.property(metaPropertyKey);
            if (!arePropertiesTheSame(leftMetaProp, rightMetaProp)) {
                // meta-properties are different -> properties are different.
                return false;
            }
        }
        return true;
    }

    private void mergeEdgeChangesFrom(final GraphTransactionContext oldContext) {
        Set<ChronoEdge> edgesToSynchronize = Sets.newHashSet(oldContext.getModifiedEdges());
        for (ChronoEdge edge : edgesToSynchronize) {
            ElementLifecycleStatus status = edge.getStatus();
            switch (status) {
                case NEW: {
                    // try to get the edge from the store
                    Edge storeEdge = Iterators.getOnlyElement(this.graph.edges(edge.id()), null);
                    if (storeEdge == null) {
                        Vertex outV = Iterators.getOnlyElement(this.graph.vertices(edge.outVertex().id()), null);
                        Vertex inV = Iterators.getOnlyElement(this.graph.vertices(edge.inVertex().id()), null);
                        if (outV == null || inV == null) {
                            // edge can not be created because one of the vertex ends does not exist in merged state
                            break;
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
                    Edge storeEdge = Iterators.getOnlyElement(this.graph.edges(edge.id()), null);
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
                    Edge storeEdge = Iterators.getOnlyElement(this.graph.edges(edge.id()), null);
                    if (storeEdge != null) {
                        // for edges, PROPERTY_CHANGED can also mean that the in/out vertex or label has changed!
                        if (!storeEdge.inVertex().id().equals(edge.inVertex().id())
                            || !storeEdge.outVertex().id().equals(edge.outVertex().id())
                            || !storeEdge.label().equals(edge.label())
                        ) {
                            // recreate the edge in the store
                            Vertex storeOutV = this.graph.vertex(edge.outVertex().id());
                            Vertex storeInV = this.graph.vertex(edge.inVertex().id());
                            storeEdge.remove();
                            if (storeOutV == null || storeInV == null) {
                                // cannot recreate edge -> store is lacking one of its adjacent vertices
                                break;
                            }
                            Edge newStoreEdge = storeOutV.addEdge(edge.label(), storeInV, T.id, edge.id());
                            // copy all property values
                            Set<String> propertyKeys = edge.keys();
                            for (String propertyKey : propertyKeys) {
                                Property<?> prop = edge.property(propertyKey);
                                if (prop.isPresent()) {
                                    newStoreEdge.property(propertyKey, prop.value());
                                }
                            }
                        } else {
                            // the edge exists in the store, check the properties
                            Set<String> propertyKeys = Sets.newHashSet();
                            propertyKeys.addAll(edge.keys());
                            propertyKeys.addAll(storeEdge.keys());
                            for (String propertyKey : propertyKeys) {
                                PropertyStatus propertyStatus = edge.getPropertyStatus(propertyKey);
                                switch (propertyStatus) {
                                    case NEW:
                                        // FALL THROUGH
                                    case MODIFIED:
                                        Property<?> txProperty = edge.property(propertyKey);
                                        Property<?> storeProperty = storeEdge.property(propertyKey);
                                        if (!arePropertiesTheSame(txProperty, storeProperty)) {
                                            storeEdge.property(propertyKey, txProperty.value());
                                        }
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
                    }
                    break;
                }
                default:
                    throw new UnknownEnumLiteralException(status);
            }
        }
    }


    private void mergeGraphVariableChangesFrom(final GraphTransactionContext oldContext) {
        // get the backing transaction
        Set<String> keyspaces = oldContext.getModifiedVariableKeyspaces();
        ChronoGraphVariables storeVariables = this.graph.variables();
        for (String keyspace : keyspaces) {
            // write the modifications of graph variables into the transaction context
            for (String variableName : oldContext.getModifiedVariables(keyspace)) {
                Optional<Object> storeVariable = storeVariables.get(keyspace, variableName);
                Object newValue = oldContext.getModifiedVariableValue(keyspace, variableName);
                if (storeVariable.isPresent()) {
                    if (oldContext.isVariableRemoved(keyspace, variableName)) {
                        // did exist before, was removed
                        this.graph.variables().remove(keyspace, variableName);
                    } else {
                        // did exist before, still exists. Check if values are identical
                        if (!Objects.equal(storeVariable.get(), newValue)) {
                            // value really changed, apply the change
                            this.graph.variables().set(keyspace, variableName, newValue);
                        }
                        // otherwise the value was changed and reverted within the same transaction, e.g. a->b->a.
                        // we ignore those changes on purpose.
                    }
                } else {
                    if (!oldContext.isVariableRemoved(keyspace, variableName)) {
                        // did not exist before, was created
                        this.graph.variables().set(keyspace, variableName, newValue);
                    }
                    // otherwise the variable did not exist before, was created and removed within same transaction -> no-op.
                }
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

    private void validateGraphInvariant() {
        try {
            this.context.getModifiedVertices().forEach(v -> ((ChronoElementInternal) v).validateGraphInvariant());
            this.context.getModifiedEdges().forEach(e -> ((ChronoElementInternal) e).validateGraphInvariant());
        } catch (GraphInvariantViolationException e) {
            throw new GraphInvariantViolationException("A Graph Invariant Violation has been detected. Transaction details: Coords: [" + this.getBranchName() + "@" + this.getTimestamp() + "] TxID " + this.transactionId, e);
        }
    }

    private SchemaValidationResult performGraphSchemaValidation() {
        String branchName = this.getBranchName();
        Set<ChronoVertex> modifiedVertices = this.context.getModifiedVertices().stream()
            // there is no point in validating deleted elements, so we filter them
            .filter(v -> !v.isRemoved())
            // we want the validator to work on a read-only version
            .map(ReadOnlyChronoVertex::new)
            .collect(Collectors.toSet());
        Set<ChronoEdge> modifiedEdges = this.context.getModifiedEdges().stream()
            // there is no point in validating deleted elements, so we filter them
            .filter(e -> !e.isRemoved())
            // we want the validator to work on a read-only version
            .map(ReadOnlyChronoEdge::new)
            .collect(Collectors.toSet());
        Set<ChronoElement> modifiedElements = Sets.union(modifiedVertices, modifiedEdges);
        return this.getGraph().getSchemaManager().validate(branchName, modifiedElements);
    }

    private void mapModifiedVerticesToChronoDB() {
        // get the backing transaction
        ChronoDBTransaction tx = this.getBackingDBTransaction();
        // read the set of modified vertices
        Set<ChronoVertex> modifiedVertices = this.context.getModifiedVertices();
        // write each vertex into a key-value pair in the transaction
        for (ChronoVertex vertex : modifiedVertices) {
            String vertexId = vertex.id();
            ElementLifecycleStatus vertexStatus = vertex.getStatus();
            switch (vertexStatus) {
                case NEW:
                    tx.put(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, ((ChronoVertexImpl) vertex).toRecord());
                    break;
                case OBSOLETE:
                    // obsolete graph elements are not committed to the store,
                    // they have been created AND removed in the same transaction
                    break;
                case EDGE_CHANGED:
                    tx.put(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, ((ChronoVertexImpl) vertex).toRecord(), PutOption.NO_INDEX);
                    break;
                case PERSISTED:
                    // this case should actually be unreachable because persisted elements are clean and not dirty
                    throw new IllegalStateException(
                        "Unreachable code reached: PERSISTED vertex '" + vertexId + "' is listed as dirty!");
                case PROPERTY_CHANGED:
                    tx.put(ChronoGraphConstants.KEYSPACE_VERTEX, vertexId, ((ChronoVertexImpl) vertex).toRecord());
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
        Set<ChronoEdge> modifiedEdges = this.context.getModifiedEdges();
        // write each edge into a key-value pair in the transaction
        for (ChronoEdge edge : modifiedEdges) {
            String edgeId = edge.id();
            ElementLifecycleStatus edgeStatus = edge.getStatus();
            switch (edgeStatus) {
                case NEW:
                    if (log.isTraceEnabled()) {
                        log.trace("[COMMIT]: Committing Edge '" + edgeId + "' in status NEW");
                    }
                    tx.put(ChronoGraphConstants.KEYSPACE_EDGE, edgeId, ((ChronoEdgeImpl) edge).toRecord());
                    break;
                case EDGE_CHANGED:
                    throw new IllegalStateException(
                        "Unreachable code reached: Detected edge '" + edgeId + "' in state EDGE_CHANGED!");
                case OBSOLETE:
                    if (log.isTraceEnabled()) {
                        log.trace("[COMMIT]: Ignoring Edge '" + edgeId + "' in status OBSOLETE");
                    }
                    // obsolete graph elements are not committed to the store,
                    // they have been created AND removed in the same transaction
                    break;
                case PERSISTED:
                    throw new IllegalStateException(
                        "Unreachable code reached: PERSISTED edge '" + edgeId + "' is listed as dirty!");
                case PROPERTY_CHANGED:
                    if (log.isTraceEnabled()) {
                        log.trace("[COMMIT]: Committing Edge '" + edgeId + "' in status PROPERTY_CHANGED");
                    }
                    tx.put(ChronoGraphConstants.KEYSPACE_EDGE, edgeId, ((ChronoEdgeImpl) edge).toRecord());
                    break;
                case REMOVED:
                    if (log.isTraceEnabled()) {
                        log.trace("[COMMIT]: Removing Edge '" + edgeId + "' in status REMOVED");
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
        // read the modified graph variables
        Set<String> modifiedVariableKeyspaces = this.context.getModifiedVariableKeyspaces();
        for (String keyspace : modifiedVariableKeyspaces) {
            String chronoDbKeyspace = ChronoGraphVariablesImpl.createChronoDBVariablesKeyspace(keyspace);
            for (String key : this.context.getModifiedVariables(keyspace)) {
                if (this.context.isVariableRemoved(keyspace, key)) {
                    tx.remove(chronoDbKeyspace, key);
                } else {
                    tx.put(chronoDbKeyspace, key, this.context.getModifiedVariableValue(keyspace, key));
                }
            }
        }
    }

    // =================================================================================================================
    // COMMIT TRIGGER METHODS
    // =================================================================================================================

    private void firePreCommitTriggers(Object commitMetadata) {
        List<Pair<String, ChronoGraphPreCommitTrigger>> triggers = this.getGraphInternal().getTriggerManager().getPreCommitTriggers();
        if (triggers.isEmpty()) {
            return;
        }
        GraphBranch branch = this.getGraph().getBranchManager().getBranch(this.getBranchName());
        try (PreCommitTriggerContext ctx = new PreTriggerContextImpl(branch, commitMetadata, this.graph, this::createAncestorGraph, this::createStoreStateGraph)) {
            for (Pair<String, ChronoGraphPreCommitTrigger> nameAndTrigger : triggers) {
                String triggerName = nameAndTrigger.getLeft();
                ChronoGraphPreCommitTrigger trigger = nameAndTrigger.getRight();
                try {
                    trigger.onPreCommit(ctx);
                } catch (CancelCommitException cancelException) {
                    throw new ChronoDBCommitException("Commit was rejected by Trigger '" + triggerName + "'!");
                } catch (Exception otherException) {
                    log.error("Exception when evaluating Trigger '" + triggerName + "' in PRE COMMIT timing. Commit will continue.", otherException);
                }
            }
        }
    }

    private void firePrePersistTriggers(Object commitMetadata) {
        List<Pair<String, ChronoGraphPrePersistTrigger>> triggers = this.getGraphInternal().getTriggerManager().getPrePersistTriggers();
        if (triggers.isEmpty()) {
            return;
        }
        GraphBranch branch = this.getGraph().getBranchManager().getBranch(this.getBranchName());
        try (PrePersistTriggerContext ctx = new PreTriggerContextImpl(branch, commitMetadata, this.graph, this::createAncestorGraph, this::createStoreStateGraph)) {
            for (Pair<String, ChronoGraphPrePersistTrigger> nameAndTrigger : triggers) {
                String triggerName = nameAndTrigger.getLeft();
                ChronoGraphPrePersistTrigger trigger = nameAndTrigger.getRight();
                try {
                    trigger.onPrePersist(ctx);
                } catch (CancelCommitException cancelException) {
                    throw new ChronoDBCommitException("Commit was rejected by Trigger '" + triggerName + "'!");
                } catch (Exception otherException) {
                    log.error("Exception when evaluating Trigger '" + triggerName + "' in PRE PERSIST timing. Commit will continue.", otherException);
                }
            }
        }
    }

    private void firePostPersistTriggers(long commitTimestamp, Object commitMetadata) {
        List<Pair<String, ChronoGraphPostPersistTrigger>> triggers = this.getGraphInternal().getTriggerManager().getPostPersistTriggers();
        if (triggers.isEmpty()) {
            return;
        }
        GraphBranch branch = this.getGraph().getBranchManager().getBranch(this.getBranchName());
        try (PostPersistTriggerContext ctx = new PostTriggerContextImpl(branch, commitTimestamp, commitMetadata, this.graph, this::createAncestorGraph, this::createStoreStateGraph, () -> this.createPreCommitStoreStateGraph(commitTimestamp))) {
            for (Pair<String, ChronoGraphPostPersistTrigger> nameAndTrigger : triggers) {
                String triggerName = nameAndTrigger.getLeft();
                ChronoGraphPostPersistTrigger trigger = nameAndTrigger.getRight();
                try {
                    trigger.onPostPersist(ctx);
                } catch (CancelCommitException cancelException) {
                    throw new ChronoDBCommitException("Commit was rejected by Trigger '" + triggerName + "'!");
                } catch (Exception otherException) {
                    log.error("Exception when evaluating Trigger '" + triggerName + "' in POST PERSIST timing. Commit will continue.", otherException);
                }
            }
        }
    }

    private void firePostCommitTriggers(long commitTimestamp, Object commitMetadata) {
        List<Pair<String, ChronoGraphPostCommitTrigger>> triggers = this.getGraphInternal().getTriggerManager().getPostCommitTriggers();
        if (triggers.isEmpty()) {
            return;
        }
        GraphBranch branch = this.getGraph().getBranchManager().getBranch(this.getBranchName());
        try (PostCommitTriggerContext ctx = new PostTriggerContextImpl(branch, commitTimestamp, commitMetadata, this.graph, this::createAncestorGraph, this::createStoreStateGraph, () -> this.createPreCommitStoreStateGraph(commitTimestamp))) {
            for (Pair<String, ChronoGraphPostCommitTrigger> nameAndTrigger : triggers) {
                String triggerName = nameAndTrigger.getLeft();
                ChronoGraphPostCommitTrigger trigger = nameAndTrigger.getRight();
                try {
                    trigger.onPostCommit(ctx);
                } catch (CancelCommitException cancelException) {
                    throw new ChronoDBCommitException("Commit was rejected by Trigger '" + triggerName + "'!");
                } catch (Exception otherException) {
                    log.error("Exception when evaluating Trigger '" + triggerName + "' in POST COMMIT timing. Commit will continue.", otherException);
                }
            }
        }
    }

    private ChronoGraph createAncestorGraph() {
        ChronoGraph sourceGraph = this.graph;
        if (this.graph instanceof ChronoThreadedTransactionGraph) {
            sourceGraph = ((ChronoThreadedTransactionGraph) this.graph).getOriginalGraph();
        }
        return sourceGraph.tx().createThreadedTx(this.getBranchName(), this.getTimestamp());
    }

    private ChronoGraph createStoreStateGraph() {
        ChronoGraph sourceGraph = this.graph;
        if (this.graph instanceof ChronoThreadedTransactionGraph) {
            sourceGraph = ((ChronoThreadedTransactionGraph) this.graph).getOriginalGraph();
        }
        return sourceGraph.tx().createThreadedTx(this.getBranchName());
    }

    private ChronoGraph createPreCommitStoreStateGraph(long timestamp) {
        ChronoGraph sourceGraph = this.graph;
        if (this.graph instanceof ChronoThreadedTransactionGraph) {
            sourceGraph = ((ChronoThreadedTransactionGraph) this.graph).getOriginalGraph();
        }
        return sourceGraph.tx().createThreadedTx(this.getBranchName(), timestamp - 1);
    }

    // =====================================================================================================================
    // DEBUG LOGGING
    // =====================================================================================================================

    private void logAddVertex(final String vertexId, final boolean isUserProvided) {
        if (!log.isTraceEnabled(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(ChronoGraphLoggingUtil.createLogHeader(this));
        messageBuilder.append("Adding Vertex with ");
        if (isUserProvided) {
            messageBuilder.append("user-provided ");
        } else {
            messageBuilder.append("auto-generated ");
        }
        messageBuilder.append("ID '");
        messageBuilder.append(vertexId);
        messageBuilder.append("' to graph.");
        log.trace(CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, messageBuilder.toString());
    }

}
