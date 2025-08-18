package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.Order;
import org.chronos.chronograph.api.branch.ChronoGraphBranchManager;
import org.chronos.chronograph.api.history.ChronoGraphHistoryManager;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.maintenance.ChronoGraphMaintenanceManager;
import org.chronos.chronograph.api.schema.ChronoGraphSchemaManager;
import org.chronos.chronograph.api.statistics.ChronoGraphStatisticsManager;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoGraphVariables;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;
import org.chronos.chronograph.api.transaction.EdgeStatus;
import org.chronos.chronograph.api.transaction.VertexStatus;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTriggerManager;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.*;

public class NoTransactionControlChronoGraph implements ChronoGraph {

    private final ChronoGraph graph;

    public NoTransactionControlChronoGraph(ChronoGraph graph) {
        this.graph = graph;
    }


    @Override
    public ChronoGraphTransactionManager tx() {
        return new NoTransactionControlTransactionManager(this.graph.tx());
    }

    @Override
    public ChronoGraphConfiguration getChronoGraphConfiguration() {
        return this.graph.getChronoGraphConfiguration();
    }

    @Override
    public void close() {
        this.graph.close();
    }

    @Override
    public ChronoGraphVariables variables() {
        return this.graph.variables();
    }

    @Override
    public Configuration configuration() {
        return this.graph.configuration();
    }

    @Override
    public boolean isClosed() {
        return this.graph.isClosed();
    }

    @Override
    public long getNow() {
        return this.graph.getNow();
    }

    @Override
    public long getNow(final String branchName) {
        return this.graph.getNow(branchName);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        return this.graph.addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        return this.graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return this.graph.compute();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return this.graph.vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return this.graph.edges(edgeIds);
    }

    @Override
    public VertexStatus vertexStatus(final String id) {
        return this.graph.vertexStatus(id);
    }

    @Override
    public EdgeStatus edgeStatus(final String id) {
        return this.graph.edgeStatus(id);
    }

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        return this.graph.getVertexHistory(vertexId, lowerBound, upperBound, order);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object edgeId, final long lowerBound, final long upperBound, final Order order) {
        return this.graph.getEdgeHistory(edgeId, lowerBound, upperBound, order);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Edge edge) {
        return this.graph.getEdgeHistory(edge);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Vertex vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        return this.graph.getLastModificationTimestampOfVertex(vertex);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Object vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        return this.graph.getLastModificationTimestampOfVertex(vertexId);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        return this.graph.getLastModificationTimestampOfEdge(edge);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Object edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        return this.graph.getLastModificationTimestampOfEdge(edgeId);
    }

    @Override
    public Iterator<Pair<Long, String>> getVertexModificationsBetween(final long timestampLowerBound, final long timestampUpperBound) {
        return this.graph.getVertexModificationsBetween(timestampLowerBound, timestampUpperBound);
    }

    @Override
    public Iterator<Pair<Long, String>> getEdgeModificationsBetween(final long timestampLowerBound, final long timestampUpperBound) {
        return this.graph.getEdgeModificationsBetween(timestampLowerBound, timestampUpperBound);
    }

    @Override
    public Object getCommitMetadata(final String branch, final long timestamp) {
        return this.graph.getCommitMetadata(branch, timestamp);
    }

    @Override
    public Iterator<Long> getCommitTimestampsBetween(final String branch, final long from, final long to, final Order order, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitTimestampsBetween(branch, from, to, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final String branch, final long from, final long to, final Order order, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitMetadataBetween(branch, from, to, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Long> getCommitTimestampsPaged(final String branch, final long minTimestamp, final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitTimestampsPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final String branch, final long minTimestamp, final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitMetadataPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex, order, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataAround(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitMetadataAround(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataBefore(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitMetadataBefore(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataAfter(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitMetadataAfter(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsAround(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitTimestampsAround(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsBefore(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitTimestampsBefore(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsAfter(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        return this.graph.getCommitTimestampsAfter(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public int countCommitTimestampsBetween(final String branch, final long from, final long to, final boolean includeSystemInternalCommits) {
        return this.graph.countCommitTimestampsBetween(branch, from, to, includeSystemInternalCommits);
    }

    @Override
    public int countCommitTimestamps(final String branch, boolean includeSystemInternalCommits) {
        return this.graph.countCommitTimestamps(branch, includeSystemInternalCommits);
    }

    @Override
    public Iterator<String> getChangedVerticesAtCommit(final String branch, final long commitTimestamp) {
        return this.graph.getChangedVerticesAtCommit(branch, commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedEdgesAtCommit(final String branch, final long commitTimestamp) {
        return this.graph.getChangedEdgesAtCommit(branch, commitTimestamp);
    }

    @Override
    public Iterator<Pair<String, String>> getChangedGraphVariablesAtCommit(final long commitTimestamp) {
        return this.graph.getChangedGraphVariablesAtCommit(commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInDefaultKeyspace(final long commitTimestamp) {
        return this.graph.getChangedGraphVariablesAtCommitInDefaultKeyspace(commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInKeyspace(final long commitTimestamp, final String keyspace) {
        return this.graph.getChangedGraphVariablesAtCommitInKeyspace(commitTimestamp, keyspace);
    }

    @Override
    public Iterator<Pair<String, String>> getChangedGraphVariablesAtCommit(final String branch, final long commitTimestamp) {
        return this.graph.getChangedGraphVariablesAtCommit(branch, commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInDefaultKeyspace(final String branch, final long commitTimestamp) {
        return this.graph.getChangedGraphVariablesAtCommitInDefaultKeyspace(branch, commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInKeyspace(final String branch, final long commitTimestamp, final String keyspace) {
        return this.graph.getChangedGraphVariablesAtCommitInKeyspace(branch, commitTimestamp, keyspace);
    }

    @Override
    public ChronoGraphIndexManager getIndexManagerOnMaster() {
        return this.graph.getIndexManagerOnMaster();
    }

    @Override
    public ChronoGraphIndexManager getIndexManagerOnBranch(final String branchName) {
        return this.graph.getIndexManagerOnBranch(branchName);
    }

    @Override
    public ChronoGraphBranchManager getBranchManager() {
        return this.graph.getBranchManager();
    }

    @Override
    public ChronoGraphTriggerManager getTriggerManager() {
        return this.graph.getTriggerManager();
    }

    @Override
    public ChronoGraphSchemaManager getSchemaManager() {
        return this.graph.getSchemaManager();
    }

    @Override
    public ChronoGraphStatisticsManager getStatisticsManager() {
        return this.graph.getStatisticsManager();
    }

    @Override
    public ChronoGraphMaintenanceManager getMaintenanceManager() {
        return this.graph.getMaintenanceManager();
    }

    @Override
    public ChronoGraphHistoryManager getHistoryManager() {
        return this.graph.getHistoryManager();
    }

    @Override
    public void writeDump(final File dumpFile, final DumpOption... dumpOptions) {
        this.graph.writeDump(dumpFile, dumpOptions);
    }

    @Override
    public void readDump(final File dumpFile, final DumpOption... options) {
        this.graph.readDump(dumpFile, options);
    }
}
