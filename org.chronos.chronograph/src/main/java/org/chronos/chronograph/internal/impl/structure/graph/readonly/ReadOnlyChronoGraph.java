package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import com.google.common.collect.Iterators;
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
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoGraphVariables;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;
import org.chronos.chronograph.api.transaction.EdgeStatus;
import org.chronos.chronograph.api.transaction.VertexStatus;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTriggerManager;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraph implements ChronoGraph {

    private ChronoGraph graph;

    public ReadOnlyChronoGraph(ChronoGraph graph) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        this.graph = graph;
    }

    @Override
    public ChronoGraphConfiguration getChronoGraphConfiguration() {
        return this.graph.getChronoGraphConfiguration();
    }

    @Override
    public void close() {
        this.unsupportedOperation();
    }

    @Override
    public ChronoGraphVariables variables() {
        return new ReadOnlyVariables(this.graph.variables());
    }

    @Override
    public Configuration configuration() {
        return new ReadOnlyConfiguration(this.graph.configuration());
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
        return this.unsupportedOperation();
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        Iterator<ChronoVertex> vertices = (Iterator) this.graph.vertices(vertexIds);
        return Iterators.transform(vertices, ReadOnlyChronoVertex::new);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Iterator<Edge> edges(final Object... edgeIds) {
        Iterator<ChronoEdge> edges = (Iterator) this.graph.edges(edgeIds);
        return Iterators.transform(edges, ReadOnlyChronoEdge::new);
    }

    @Override
    public VertexStatus vertexStatus(final String id) {
        var status = this.graph.vertexStatus(id);
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
    public EdgeStatus edgeStatus(final String id) {
        var status = this.graph.edgeStatus(id);
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
    public ChronoGraphTransactionManager tx() {
        return new ReadOnlyChronoGraphTransactionManager(this.graph.tx());
    }

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        Iterator<Long> history = this.graph.getVertexHistory(vertexId, lowerBound, upperBound, order);
        return Iterators.unmodifiableIterator(history);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object edgeId) {
        Iterator<Long> history = this.graph.getEdgeHistory(edgeId);
        return Iterators.unmodifiableIterator(history);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object edgeId, final long lowerBound, final long upperBound, final Order order) {
        Iterator<Long> history = this.graph.getEdgeHistory(edgeId, lowerBound, upperBound, order);
        return Iterators.unmodifiableIterator(history);
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
        Iterator<Pair<Long, String>> modifications = this.graph.getVertexModificationsBetween(timestampLowerBound, timestampUpperBound);
        return Iterators.unmodifiableIterator(modifications);
    }

    @Override
    public Iterator<Pair<Long, String>> getEdgeModificationsBetween(final long timestampLowerBound, final long timestampUpperBound) {
        Iterator<Pair<Long, String>> modifications = this.graph.getEdgeModificationsBetween(timestampLowerBound, timestampUpperBound);
        return Iterators.unmodifiableIterator(modifications);
    }

    @Override
    public Object getCommitMetadata(final String branch, final long timestamp) {
        return this.graph.getCommitMetadata(branch, timestamp);
    }

    @Override
    public Iterator<Long> getCommitTimestampsBetween(final String branch, final long from, final long to, final Order order, final boolean includeSystemInternalCommits) {
        Iterator<Long> timestamps = this.graph.getCommitTimestampsBetween(branch, from, to, order, includeSystemInternalCommits);
        return Iterators.unmodifiableIterator(timestamps);
    }

    @Override
    public Iterator<Map.Entry<Long, Object>> getCommitMetadataBetween(final String branch, final long from, final long to, final Order order, final boolean includeSystemInternalCommits) {
        Iterator<Entry<Long, Object>> metadata = this.graph.getCommitMetadataBetween(branch, from, to, order, includeSystemInternalCommits);
        return Iterators.unmodifiableIterator(metadata);
    }

    @Override
    public Iterator<Long> getCommitTimestampsPaged(final String branch, final long minTimestamp, final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        Iterator<Long> timestamps = this.graph.getCommitTimestampsPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex, order, includeSystemInternalCommits);
        return Iterators.unmodifiableIterator(timestamps);
    }

    @Override
    public Iterator<Map.Entry<Long, Object>> getCommitMetadataPaged(final String branch, final long minTimestamp, final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        Iterator<Entry<Long, Object>> metadata = this.graph.getCommitMetadataPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex, order, includeSystemInternalCommits);
        return Iterators.unmodifiableIterator(metadata);
    }

    @Override
    public List<Map.Entry<Long, Object>> getCommitMetadataAround(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        List<Entry<Long, Object>> metadata = this.graph.getCommitMetadataAround(branch, timestamp, count, includeSystemInternalCommits);
        return Collections.unmodifiableList(metadata);
    }

    @Override
    public List<Map.Entry<Long, Object>> getCommitMetadataBefore(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        List<Entry<Long, Object>> metadata = this.graph.getCommitMetadataBefore(branch, timestamp, count, includeSystemInternalCommits);
        return Collections.unmodifiableList(metadata);
    }

    @Override
    public List<Map.Entry<Long, Object>> getCommitMetadataAfter(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        List<Entry<Long, Object>> metadata = this.graph.getCommitMetadataAfter(branch, timestamp, count, includeSystemInternalCommits);
        return Collections.unmodifiableList(metadata);
    }

    @Override
    public List<Long> getCommitTimestampsAround(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        List<Long> timestamps = this.graph.getCommitTimestampsAround(branch, timestamp, count, includeSystemInternalCommits);
        return Collections.unmodifiableList(timestamps);
    }

    @Override
    public List<Long> getCommitTimestampsBefore(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        List<Long> timestamps = this.graph.getCommitTimestampsBefore(branch, timestamp, count, includeSystemInternalCommits);
        return Collections.unmodifiableList(timestamps);
    }

    @Override
    public List<Long> getCommitTimestampsAfter(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        List<Long> timestamps = this.graph.getCommitTimestampsAfter(branch, timestamp, count, includeSystemInternalCommits);
        return Collections.unmodifiableList(timestamps);
    }

    @Override
    public int countCommitTimestampsBetween(final String branch, final long from, final long to, final boolean includeSystemInternalCommits) {
        return this.graph.countCommitTimestampsBetween(branch, from, to, includeSystemInternalCommits);
    }

    @Override
    public int countCommitTimestamps(final String branch, final boolean includeSystemInternalCommits) {
        return this.graph.countCommitTimestamps(branch, includeSystemInternalCommits);
    }

    @Override
    public Iterator<String> getChangedVerticesAtCommit(final String branch, final long commitTimestamp) {
        Iterator<String> vertexIds = this.graph.getChangedVerticesAtCommit(branch, commitTimestamp);
        return Iterators.unmodifiableIterator(vertexIds);
    }

    @Override
    public Iterator<String> getChangedEdgesAtCommit(final String branch, final long commitTimestamp) {
        Iterator<String> edgeIds = this.graph.getChangedEdgesAtCommit(branch, commitTimestamp);
        return Iterators.unmodifiableIterator(edgeIds);
    }

    @Override
    public Iterator<Pair<String, String>> getChangedGraphVariablesAtCommit(final long commitTimestamp) {
        return Iterators.unmodifiableIterator(this.graph.getChangedGraphVariablesAtCommit(commitTimestamp));
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInDefaultKeyspace(final long commitTimestamp) {
        return Iterators.unmodifiableIterator(this.graph.getChangedGraphVariablesAtCommitInDefaultKeyspace(commitTimestamp));
    }


    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInKeyspace(final long commitTimestamp, final String keyspace) {
        return Iterators.unmodifiableIterator(this.graph.getChangedGraphVariablesAtCommitInKeyspace(commitTimestamp, keyspace));
    }

    @Override
    public Iterator<Pair<String, String>> getChangedGraphVariablesAtCommit(final String branch, final long commitTimestamp) {
        return Iterators.unmodifiableIterator(this.graph.getChangedGraphVariablesAtCommit(branch, commitTimestamp));
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInDefaultKeyspace(final String branch, final long commitTimestamp) {
        return Iterators.unmodifiableIterator(this.graph.getChangedGraphVariablesAtCommitInDefaultKeyspace(branch, commitTimestamp));
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInKeyspace(final String branch, final long commitTimestamp, final String keyspace) {
        return Iterators.unmodifiableIterator(this.graph.getChangedGraphVariablesAtCommitInKeyspace(branch, commitTimestamp, keyspace));
    }

    @Override
    public ChronoGraphIndexManager getIndexManagerOnMaster() {
        return new ReadOnlyChronoGraphIndexManager(this.graph.getIndexManagerOnMaster());
    }

    @Override
    public ChronoGraphIndexManager getIndexManagerOnBranch(final String branchName) {
        return new ReadOnlyChronoGraphIndexManager(this.graph.getIndexManagerOnBranch(branchName));
    }

    @Override
    public ChronoGraphBranchManager getBranchManager() {
        return new ReadOnlyChronoGraphBranchManager(this.graph.getBranchManager());
    }

    @Override
    public ChronoGraphTriggerManager getTriggerManager() {
        return new ReadOnlyChronoGraphTriggerManager(this.graph.getTriggerManager());
    }

    @Override
    public ChronoGraphSchemaManager getSchemaManager() {
        return new ReadOnlyChronoGraphSchemaManager(this.graph.getSchemaManager());
    }

    @Override
    public ChronoGraphMaintenanceManager getMaintenanceManager() {
        return new ReadOnlyChronoGraphMaintenanceManager(this.graph.getMaintenanceManager());
    }

    @Override
    public ChronoGraphStatisticsManager getStatisticsManager() {
        return new ReadOnlyChronoGraphStatisticsManager(this.graph.getStatisticsManager());
    }

    @Override
    public ChronoGraphHistoryManager getHistoryManager() {
        return new ReadOnlyHistoryManager(this.graph.getHistoryManager());
    }

    @Override
    public void writeDump(final File dumpFile, final DumpOption... dumpOptions) {
        this.unsupportedOperation();
    }

    @Override
    public void readDump(final File dumpFile, final DumpOption... options) {
        this.unsupportedOperation();
    }

    private <T> T unsupportedOperation() {
        throw new UnsupportedOperationException("This operation is not supported on a readOnly graph!");
    }
}
