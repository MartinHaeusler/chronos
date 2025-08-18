package org.chronos.chronograph.internal.impl.structure.graph;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.chronodb.internal.impl.engines.base.ChronosInternalCommitMetadata;
import org.chronos.chronodb.internal.util.IteratorUtils;
import org.chronos.chronograph.api.branch.ChronoGraphBranchManager;
import org.chronos.chronograph.api.history.ChronoGraphHistoryManager;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.jmx.ChronoGraphMBeanSupport;
import org.chronos.chronograph.api.maintenance.ChronoGraphMaintenanceManager;
import org.chronos.chronograph.api.schema.ChronoGraphSchemaManager;
import org.chronos.chronograph.api.statistics.ChronoGraphStatisticsManager;
import org.chronos.chronograph.api.structure.ChronoGraphVariables;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;
import org.chronos.chronograph.api.transaction.EdgeStatus;
import org.chronos.chronograph.api.transaction.VertexStatus;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.branch.ChronoGraphBranchManagerImpl;
import org.chronos.chronograph.internal.impl.configuration.ChronoGraphConfigurationImpl;
import org.chronos.chronograph.internal.impl.dumpformat.GraphDumpFormat;
import org.chronos.chronograph.internal.impl.history.ChronoGraphHistoryManagerImpl;
import org.chronos.chronograph.internal.impl.index.ChronoGraphIndexManagerImpl;
import org.chronos.chronograph.internal.impl.maintenance.ChronoGraphMaintenanceManagerImpl;
import org.chronos.chronograph.internal.impl.migration.ChronoGraphMigrationChain;
import org.chronos.chronograph.internal.impl.optimizer.strategy.*;
import org.chronos.chronograph.internal.impl.schema.ChronoGraphSchemaManagerImpl;
import org.chronos.chronograph.internal.impl.statistics.ChronoGraphStatisticsManagerImpl;
import org.chronos.chronograph.internal.impl.structure.graph.features.ChronoGraphFeatures;
import org.chronos.chronograph.internal.impl.transaction.ChronoGraphTransactionManagerImpl;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;
import org.chronos.chronograph.internal.impl.transaction.trigger.ChronoGraphTriggerManagerImpl;
import org.chronos.chronograph.internal.impl.transaction.trigger.ChronoGraphTriggerManagerInternal;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.configuration.ChronosConfigurationUtil;
import org.chronos.common.version.ChronosVersion;
import org.chronos.common.version.VersionKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class StandardChronoGraph implements ChronoGraphInternal {

    private static final Logger log = LoggerFactory.getLogger(StandardChronoGraph.class);

    static {
        TraversalStrategies graphStrategies = TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone();

        // do not use the EarlyLimitStrategy, because it has a bug when a query contains multiple ".limit(0)" steps
        // (see: https://issues.apache.org/jira/browse/TINKERPOP-2932)
        graphStrategies.removeStrategies(EarlyLimitStrategy.class);

        graphStrategies.addStrategies(
            ChronoGraphStepStrategy.INSTANCE,
            PredicateNormalizationStrategy.INSTANCE,
            ReplaceGremlinPredicateWithChronosPredicateStrategy.INSTANCE,
            FetchValuesFromSecondaryIndexStrategy.INSTANCE,
            UseSecondaryIndexForHasStepsStrategy.INSTANCE,
            OrderFiltersStrategy.INSTANCE
        );

        // Register with cache
        TraversalStrategies.GlobalCache.registerStrategies(StandardChronoGraph.class, graphStrategies);
        TraversalStrategies.GlobalCache.registerStrategies(ChronoThreadedTransactionGraph.class, graphStrategies);

        // TODO PERFORMANCE GRAPH: Titan has a second graph implementation for transactions. See next line.
        // TraversalStrategies.GlobalCache.registerStrategies(StandardTitanTx.class, graphStrategies);
    }

    private final Configuration rawConfiguration;
    private final ChronoGraphConfiguration graphConfiguration;

    private final ChronoDB database;
    private final ChronoGraphTransactionManager txManager;

    private final ChronoGraphBranchManager branchManager;

    private final ChronoGraphTriggerManagerInternal triggerManager;

    private final ChronoGraphSchemaManager schemaManager;

    private final ChronoGraphMaintenanceManager maintenanceManager;

    private final ChronoGraphStatisticsManager statisticsManager;

    private final ChronoGraphHistoryManager historyManager;


    private final Object branchLock = new Object();
    private final Map<String, ChronoGraphIndexManager> branchNameToIndexManager;

    private final ChronoGraphFeatures features;
    private final ChronoGraphVariablesImpl variables;

    private final Lock commitLock = new ReentrantLock(true);
    private final ThreadLocal<AutoLock> commitLockHolder = new ThreadLocal<>();

    public StandardChronoGraph(final ChronoDB database, final Configuration configuration) {
        checkNotNull(database, "Precondition violation - argument 'database' must not be NULL!");
        checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
        this.rawConfiguration = configuration;
        this.graphConfiguration = ChronosConfigurationUtil.build(configuration, ChronoGraphConfigurationImpl.class);
        this.database = database;
        this.txManager = new ChronoGraphTransactionManagerImpl(this);
        this.branchManager = new ChronoGraphBranchManagerImpl(this);
        this.schemaManager = new ChronoGraphSchemaManagerImpl(this);
        this.triggerManager = new ChronoGraphTriggerManagerImpl(this);
        this.maintenanceManager = new ChronoGraphMaintenanceManagerImpl(this);
        this.statisticsManager = new ChronoGraphStatisticsManagerImpl(this);
        this.historyManager = new ChronoGraphHistoryManagerImpl(this);
        this.branchNameToIndexManager = Maps.newHashMap();
        this.features = new ChronoGraphFeatures(this);
        this.variables = new ChronoGraphVariablesImpl(this);
        this.writeCurrentChronoGraphVersionIfNecessary();
        ChronoGraphMigrationChain.executeMigrationChainOnGraph(this);
        if (this.database.getConfiguration().isMBeanIntegrationEnabled()) {
            ChronoGraphMBeanSupport.registerMBeans(this);
        }
    }

    // =================================================================================================================
    // GRAPH CLOSING
    // =================================================================================================================

    @Override
    public void close() {
        if (this.database.isClosed()) {
            // already closed
            return;
        }
        this.database.close();
    }

    @Override
    public boolean isClosed() {
        return this.database.isClosed();
    }

    // =====================================================================================================================
    // VERTEX & EDGE HANDLING
    // =====================================================================================================================

    @Override
    public Vertex addVertex(final Object... keyValues) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().addVertex(keyValues);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().edges(edgeIds);
    }

    @Override
    public VertexStatus vertexStatus(final String id) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().vertexStatus(id);
    }

    @Override
    public EdgeStatus edgeStatus(final String id) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().edgeStatus(id);
    }

    // =====================================================================================================================
    // COMPUTATION API
    // =====================================================================================================================

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    // =====================================================================================================================
    // TRANSACTION API
    // =====================================================================================================================

    @Override
    public ChronoGraphTransactionManager tx() {
        this.assertIsOpen();
        return this.txManager;
    }

    @Override
    public long getNow() {
        this.assertIsOpen();
        return this.getBackingDB().getBranchManager().getMasterBranch().getNow();
    }

    @Override
    public long getNow(final String branchName) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        this.assertIsOpen();
        return this.getBackingDB().getBranchManager().getBranch(branchName).getNow();
    }

    // =====================================================================================================================
    // INDEXING
    // =====================================================================================================================

    @Override
    public ChronoGraphIndexManager getIndexManagerOnMaster() {
        return this.getIndexManagerOnBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
    }

    @Override
    public ChronoGraphIndexManager getIndexManagerOnBranch(final String branchName) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        this.assertIsOpen();
        synchronized (this.branchLock) {
            if (this.getBackingDB().getBranchManager().existsBranch(branchName) == false) {
                throw new IllegalArgumentException("There is no branch named '" + branchName + "'!");
            }
            // try to retrieve a cached copy of the manager
            ChronoGraphIndexManager indexManager = this.branchNameToIndexManager.get(branchName);
            if (indexManager == null) {
                // manager not present in our cache; buildLRU it and add it to the cache
                indexManager = new ChronoGraphIndexManagerImpl(this.getBackingDB(), branchName);
                this.branchNameToIndexManager.put(branchName, indexManager);
            }
            return indexManager;
        }
    }

    // =====================================================================================================================
    // BRANCHING
    // =====================================================================================================================

    @Override
    public ChronoGraphBranchManager getBranchManager() {
        this.assertIsOpen();
        return this.branchManager;
    }

    // =================================================================================================================
    // TRIGGERS
    // =================================================================================================================

    @Override
    public ChronoGraphTriggerManagerInternal getTriggerManager() {
        this.assertIsOpen();
        return this.triggerManager;
    }

    // =====================================================================================================================
    // VARIABLES & CONFIGURATION
    // =====================================================================================================================

    @Override
    public ChronoGraphVariables variables() {
        this.assertIsOpen();
        return this.variables;
    }

    @Override
    public Configuration configuration() {
        return this.rawConfiguration;
    }

    @Override
    public ChronoGraphConfiguration getChronoGraphConfiguration() {
        return this.graphConfiguration;
    }

    // =================================================================================================================
    // STORED CHRONOGRAPH VERSION
    // =================================================================================================================

    private void writeCurrentChronoGraphVersionIfNecessary() {
        Optional<ChronosVersion> storedVersion = this.getStoredChronoGraphVersion();
        if (storedVersion.isPresent()) {
            // stored version already exists, skip
            return;
        }
        // first, check if the graph is empty
        if (this.isGraphEmpty()) {
            // the graph is empty, this is a new installation,
            // set the current Chronos version as ChronoGraph version.
            if (this.getBackingDB().getConfiguration().isReadOnly()) {
                throw new IllegalStateException("This ChronoGraph instance is read-only, but graph metadata needs to be written. " +
                    "Please open this graph in read/write mode first, then close it and open it as read-only afterwards.");
            }
            this.setStoredChronoGraphVersion(ChronosVersion.getCurrentVersion());
        } else {
            // the graph isn't empty, we assume that the latest chronograph version
            // which did not support the migration chain is used.
            if (this.getBackingDB().getConfiguration().isReadOnly()) {
                throw new IllegalStateException("This ChronoGraph instance is read-only, but graph metadata needs to be written. " +
                    "Please open this graph in read/write mode first, then close it and open it as read-only afterwards.");
            }
            this.setStoredChronoGraphVersion(new ChronosVersion(0, 11, 1, VersionKind.RELEASE));
        }
    }

    private boolean isGraphEmpty() {
        this.tx().open();
        try {
            // we start the checks with the meta objects since they are cheaper to
            // query if the graph is indeed non-empty.
            if (this.getBranchManager().getBranchNames().size() > 1) {
                // there is more than one branch => graph is not empty.
                return false;
            }
            if (!this.variables().keys().isEmpty()) {
                // at least one key has been assigned => graph is not empty.
                return false;
            }
            if (!this.getIndexManagerOnMaster().getAllIndicesAtAnyPointInTime().isEmpty()) {
                // an index has been created => graph is not empty.
                return false;
            }
            if (!this.getSchemaManager().getAllValidatorNames().isEmpty()) {
                // a schema validator has been created => graph is not empty.
                return false;
            }
            if (!this.getTriggerManager().getAllTriggers().isEmpty()) {
                // a trigger has been created => graph is not empty.
                return false;
            }
            // no meta objects exist, check if we have any vertices or edges.
            ChronoDBTransaction tx = this.getBackingDB().tx();
            if (!tx.keySet(ChronoGraphConstants.KEYSPACE_VERTEX).isEmpty()) {
                // there are vertices
                return false;
            }
            if (!tx.keySet(ChronoGraphConstants.KEYSPACE_EDGE).isEmpty()) {
                // there are edges
                return false;
            }
            // the graph is completely empty
            return true;
        } finally {
            this.tx().rollback();
        }
    }

    @Override
    public Optional<ChronosVersion> getStoredChronoGraphVersion() {
        this.assertIsOpen();
        ChronoDBTransaction tx = this.getBackingDB().tx();
        String keyspace = ChronoGraphConstants.KEYSPACE_MANAGEMENT;
        String key = ChronoGraphConstants.KEYSPACE_MANAGEMENT_KEY__CHRONOGRAPH_VERSION;
        String version = tx.get(keyspace, key);
        return Optional.ofNullable(version).map(ChronosVersion::parse);
    }

    @Override
    public void setStoredChronoGraphVersion(final ChronosVersion version) {
        checkNotNull(version, "Precondition violation - argument 'version' must not be NULL!");
        this.assertIsOpen();
        Optional<ChronosVersion> currentVersion = this.getStoredChronoGraphVersion();
        if (currentVersion.isPresent() && version.isSmallerThan(currentVersion.get())) {
            throw new IllegalArgumentException("Cannot store ChronoGraph version " + version + ": the current version is higher (" + currentVersion + ")!");
        }
        ChronoDBTransaction tx = this.getBackingDB().tx();
        String keyspace = ChronoGraphConstants.KEYSPACE_MANAGEMENT;
        String key = ChronoGraphConstants.KEYSPACE_MANAGEMENT_KEY__CHRONOGRAPH_VERSION;
        tx.put(keyspace, key, version.toString());
        String commitMessage;
        if (currentVersion.isPresent()) {
            commitMessage = "Updated stored ChronoGraph version from " + currentVersion.get() + " to " + version + ".";
        } else {
            commitMessage = "Updated stored ChronoGraph version from [not set] to " + version + ".";
        }
        tx.commit(new ChronosInternalCommitMetadata(commitMessage));
    }

    // =====================================================================================================================
    // TEMPORAL ACTIONS
    // =====================================================================================================================

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getVertexHistory(vertexId);
    }

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkArgument(lowerBound >= 0, "Precondition violation - argument 'lowerBound' must not be negative!");
        checkArgument(upperBound >= 0, "Precondition violation - argument 'upperBound' must not be negative!");
        checkArgument(lowerBound <= upperBound, "Precondition violation - argument 'lowerBound' must be less than or equal to argument 'upperBound'!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getVertexHistory(vertexId, lowerBound, upperBound, order);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object edgeId, final long lowerBound, final long upperBound, final Order order) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkArgument(lowerBound >= 0, "Precondition violation - argument 'lowerBound' must not be negative!");
        checkArgument(upperBound >= 0, "Precondition violation - argument 'upperBound' must not be negative!");
        checkArgument(lowerBound <= upperBound, "Precondition violation - argument 'lowerBound' must be less than or equal to argument 'upperBound'!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getEdgeHistory(edgeId, lowerBound, upperBound, order);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getEdgeHistory(edge);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Vertex vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getLastModificationTimestampOfVertex(vertex);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Object vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getLastModificationTimestampOfVertex(vertexId);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getLastModificationTimestampOfEdge(edge);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Object edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getLastModificationTimestampOfEdge(edgeId);
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
        this.tx().readWrite();
        checkArgument(timestampLowerBound <= this.tx().getCurrentTransaction().getTimestamp(),
            "Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
        checkArgument(timestampUpperBound <= this.tx().getCurrentTransaction().getTimestamp(),
            "Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
        return this.tx().getCurrentTransaction().getVertexModificationsBetween(timestampLowerBound,
            timestampUpperBound);
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
        this.tx().readWrite();
        checkArgument(timestampLowerBound <= this.tx().getCurrentTransaction().getTimestamp(),
            "Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
        checkArgument(timestampUpperBound <= this.tx().getCurrentTransaction().getTimestamp(),
            "Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
        return this.tx().getCurrentTransaction().getEdgeModificationsBetween(timestampLowerBound, timestampUpperBound);
    }

    @Override
    public Object getCommitMetadata(final String branch, final long timestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(timestamp <= this.getNow(branch),
            "Precondition violation - argument 'timestamp' must not be larger than the latest commit timestamp!");
        return this.getBackingDB().tx(branch).getCommitMetadata(timestamp);
    }

    @Override
    public Iterator<Long> getCommitTimestampsBetween(final String branch, final long from, final long to,
                                                     final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitTimestampsBetween(from, to, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final String branch, final long from, final long to,
                                                                  final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitMetadataBetween(from, to, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Long> getCommitTimestampsPaged(final String branch, final long minTimestamp,
                                                   final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
        checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitTimestampsPaged(minTimestamp, maxTimestamp, pageSize, pageIndex, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final String branch, final long minTimestamp,
                                                                final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
        checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitMetadataPaged(minTimestamp, maxTimestamp, pageSize, pageIndex, order, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataAround(final String branch, final long timestamp,
                                                             final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitMetadataAround(timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataBefore(final String branch, final long timestamp,
                                                             final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitMetadataBefore(timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataAfter(final String branch, final long timestamp,
                                                            final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitMetadataAfter(timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsAround(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitTimestampsAround(timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsBefore(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitTimestampsBefore(timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsAfter(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        return this.getBackingDB().tx(branch).getCommitTimestampsAfter(timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public int countCommitTimestampsBetween(final String branch, final long from, final long to, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        return this.getBackingDB().tx(branch).countCommitTimestampsBetween(from, to, includeSystemInternalCommits);
    }

    @Override
    public int countCommitTimestamps(final String branch, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        return this.getBackingDB().tx(branch).countCommitTimestamps(includeSystemInternalCommits);
    }

    @Override
    public Iterator<String> getChangedVerticesAtCommit(final String branch, final long commitTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return Iterators.unmodifiableIterator(this.getBackingDB().tx(branch).getChangedKeysAtCommit(commitTimestamp, ChronoGraphConstants.KEYSPACE_VERTEX));
    }

    @Override
    public Iterator<String> getChangedEdgesAtCommit(final String branch, final long commitTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return Iterators.unmodifiableIterator(this.getBackingDB().tx(branch).getChangedKeysAtCommit(commitTimestamp, ChronoGraphConstants.KEYSPACE_EDGE));
    }

    @Override
    public Iterator<Pair<String, String>> getChangedGraphVariablesAtCommit(final long commitTimestamp) {
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.getChangedGraphVariablesAtCommit(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInDefaultKeyspace(final long commitTimestamp) {
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.getChangedGraphVariablesAtCommitInKeyspace(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, commitTimestamp, ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInKeyspace(final long commitTimestamp, final String keyspace) {
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        return this.getChangedGraphVariablesAtCommitInKeyspace(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, commitTimestamp, keyspace);
    }

    @Override
    public Iterator<Pair<String, String>> getChangedGraphVariablesAtCommit(final String branch, final long commitTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        ChronoDBTransaction tx = this.getBackingDB().tx(branch, commitTimestamp);
        Set<String> keyspaces = tx.keyspaces().stream().filter(ks -> ks.startsWith(ChronoGraphConstants.KEYSPACE_VARIABLES)).collect(Collectors.toSet());
        return Iterators.unmodifiableIterator(keyspaces.stream().flatMap(keyspace ->
            IteratorUtils.stream(tx.getChangedKeysAtCommit(commitTimestamp, keyspace))
                .map(key -> Pair.of(ChronoGraphVariablesImpl.createChronoGraphVariablesKeyspace(keyspace), key))
        ).iterator());
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInDefaultKeyspace(final String branch, final long commitTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.getChangedGraphVariablesAtCommitInKeyspace(branch, commitTimestamp, ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInKeyspace(final String branch, final long commitTimestamp, final String keyspace) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        ChronoDBTransaction tx = this.getBackingDB().tx(branch, commitTimestamp);
        return Iterators.unmodifiableIterator(tx.getChangedKeysAtCommit(commitTimestamp, ChronoGraphVariablesImpl.createChronoDBVariablesKeyspace(keyspace)));
    }

    // =================================================================================================================
    // SCHEMA MANAGEMENT
    // =================================================================================================================

    @Override
    public ChronoGraphSchemaManager getSchemaManager() {
        return this.schemaManager;
    }

    // =================================================================================================================
    // MAINTENANCE
    // =================================================================================================================

    @Override
    public ChronoGraphMaintenanceManager getMaintenanceManager() {
        return this.maintenanceManager;
    }

    // =================================================================================================================
    // STATISTICS
    // =================================================================================================================

    @Override
    public ChronoGraphStatisticsManager getStatisticsManager() {
        return this.statisticsManager;
    }

    // =================================================================================================================
    // HISTORY
    // =================================================================================================================

    @Override
    public ChronoGraphHistoryManager getHistoryManager() {
        return historyManager;
    }


    // =====================================================================================================================
    // SERIALIZATION & DESERIALIZATION (GraphSon, Gyro, ...)
    // =====================================================================================================================

    // not implemented yet

    // =====================================================================================================================
    // DUMP OPERATIONS
    // =====================================================================================================================

    @Override
    public void writeDump(final File dumpFile, final DumpOption... dumpOptions) {
        DumpOptions options = new DumpOptions(dumpOptions);
        GraphDumpFormat.registerGraphAliases(options);
        GraphDumpFormat.registerDefaultConvertersForWriting(options);
        this.getBackingDB().writeDump(dumpFile, options.toArray());
    }

    @Override
    public void readDump(final File dumpFile, final DumpOption... dumpOptions) {
        DumpOptions options = new DumpOptions(dumpOptions);
        GraphDumpFormat.registerGraphAliases(options);
        GraphDumpFormat.registerDefaultConvertersForReading(options);
        // we need to perform the ChronoGraph migrations also on the dump file.
        File migratedDumpFile = ChronoGraphMigrationChain.executeMigrationChainOnDumpFile(dumpFile, options);
        try {
            this.getBackingDB().getBackupManager().readDump(migratedDumpFile, options.toArray());
        } finally {
            boolean deleted = migratedDumpFile.delete();
            if (!deleted) {
                log.warn("Failed to delete temporary dump file: '" + migratedDumpFile.getAbsolutePath() + "'!");
            }
        }
    }

    // =====================================================================================================================
    // STRING REPRESENTATION
    // =====================================================================================================================

    @Override
    public String toString() {
        // according to Tinkerpop specification...
        return StringFactory.graphString(this, "");
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    @Override
    public ChronoDB getBackingDB() {
        this.assertIsOpen();
        return this.database;
    }

    public AutoLock commitLock() {
        AutoLock autoLock = this.commitLockHolder.get();
        if (autoLock == null) {
            autoLock = AutoLock.createBasicLockHolderFor(this.commitLock);
            this.commitLockHolder.set(autoLock);
        }
        // autoLock.releaseLock() is called on lockHolder.close()
        autoLock.acquireLock();
        return autoLock;
    }

    // =====================================================================================================================
    // FEATURES DECLARATION
    // =====================================================================================================================

    @Override
    public ChronoGraphFeatures features() {
        return this.features;
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private void assertIsOpen() {
        if (this.isClosed()) {
            throw new IllegalStateException("This ChronoGraph instance has already been closed.");
        }
    }
}
