package org.chronos.chronograph.internal.impl.structure.graph;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.chronograph.api.branch.ChronoGraphBranchManager;
import org.chronos.chronograph.api.builder.query.GraphQueryBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.branch.ChronoGraphBranchManagerImpl;
import org.chronos.chronograph.internal.impl.builder.query.GraphQueryBuilderStarterImpl;
import org.chronos.chronograph.internal.impl.configuration.ChronoGraphConfigurationImpl;
import org.chronos.chronograph.internal.impl.dumpformat.GraphDumpFormat;
import org.chronos.chronograph.internal.impl.index.ChronoGraphIndexManagerImpl;
import org.chronos.chronograph.internal.impl.optimizer.strategy.ChronoGraphStepStrategy;
import org.chronos.chronograph.internal.impl.structure.graph.features.ChronoGraphFeatures;
import org.chronos.chronograph.internal.impl.transaction.ChronoGraphTransactionManagerImpl;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.configuration.ChronosConfigurationUtil;

import com.google.common.collect.Maps;

public class StandardChronoGraph implements ChronoGraphInternal {

	static {
		TraversalStrategies graphStrategies = TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone();
		graphStrategies.addStrategies(ChronoGraphStepStrategy.getInstance());
		// TODO PERFORMANCE GRAPH: Titan has a couple more optimizations. See next line.
		// Take a look at: AdjacentVertexFilterOptimizerStrategy, TitanLocalQueryOptimizerStrategy

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

	private final Lock branchLock;
	private final Map<String, ChronoGraphIndexManager> branchNameToIndexManager;

	private final ChronoGraphFeatures features;
	private final ChronoGraphVariables variables;

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
		this.branchLock = new ReentrantLock(true);
		this.branchNameToIndexManager = Maps.newHashMap();
		this.features = new ChronoGraphFeatures(this);
		this.variables = new ChronoGraphVariables(this);
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
		return this.txManager;
	}

	@Override
	public long getNow() {
		return this.getBackingDB().getBranchManager().getMasterBranch().getNow();
	}

	@Override
	public long getNow(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		return this.getBackingDB().getBranchManager().getBranch(branchName).getNow();
	}

	// =====================================================================================================================
	// INDEXING
	// =====================================================================================================================

	@Override
	public ChronoGraphIndexManager getIndexManager() {
		return this.getIndexManager(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	@Override
	public ChronoGraphIndexManager getIndexManager(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.branchLock.lock();
		try {
			if (this.getBackingDB().getBranchManager().existsBranch(branchName) == false) {
				throw new IllegalArgumentException("There is no branch named '" + branchName + "'!");
			}
			// try to retrieve a cached copy of the manager
			ChronoGraphIndexManager indexManager = this.branchNameToIndexManager.get(branchName);
			if (indexManager == null) {
				// manager not present in our cache; buildLRU it and add it to the cache
				indexManager = new ChronoGraphIndexManagerImpl(this, branchName);
				this.branchNameToIndexManager.put(branchName, indexManager);
			}
			return indexManager;
		} finally {
			this.branchLock.unlock();
		}
	}

	// =====================================================================================================================
	// BRANCHING
	// =====================================================================================================================

	@Override
	public ChronoGraphBranchManager getBranchManager() {
		return this.branchManager;
	}

	// =================================================================================================================
	// QUERY METHODS
	// =================================================================================================================

	@Override
	public GraphQueryBuilderStarter find() {
		return new GraphQueryBuilderStarterImpl(this);
	}

	// =====================================================================================================================
	// VARIABLES & CONFIGURATION
	// =====================================================================================================================

	@Override
	public Variables variables() {
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
	public Iterator<Long> getVertexHistory(final Vertex vertex) {
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		this.tx().readWrite();
		return this.tx().getCurrentTransaction().getVertexHistory(vertex);
	}

	@Override
	public Iterator<Long> getEdgeHistory(final Object edgeId) {
		checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
		this.tx().readWrite();
		return this.tx().getCurrentTransaction().getEdgeHistory(edgeId);
	}

	@Override
	public Iterator<Long> getEdgeHistory(final Edge edge) {
		checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
		this.tx().readWrite();
		return this.tx().getCurrentTransaction().getEdgeHistory(edge);
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
			final Order order) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitTimestampsBetween(from, to, order);
	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final String branch, final long from, final long to,
			final Order order) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitMetadataBetween(from, to, order);
	}

	@Override
	public Iterator<Long> getCommitTimestampsPaged(final String branch, final long minTimestamp,
			final long maxTimestamp, final int pageSize, final int pageIndex, final Order order) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
		checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
		checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
		checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitTimestampsPaged(minTimestamp, maxTimestamp, pageSize, pageIndex,
				order);
	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final String branch, final long minTimestamp,
			final long maxTimestamp, final int pageSize, final int pageIndex, final Order order) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
		checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
		checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
		checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitMetadataPaged(minTimestamp, maxTimestamp, pageSize, pageIndex,
				order);
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataAround(final String branch, final long timestamp,
			final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitMetadataAround(timestamp, count);
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataBefore(final String branch, final long timestamp,
			final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitMetadataBefore(timestamp, count);
	}

	@Override
	public List<Entry<Long, Object>> getCommitMetadataAfter(final String branch, final long timestamp,
			final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitMetadataAfter(timestamp, count);
	}

	@Override
	public List<Long> getCommitTimestampsAround(final String branch, final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitTimestampsAround(timestamp, count);
	}

	@Override
	public List<Long> getCommitTimestampsBefore(final String branch, final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitTimestampsBefore(timestamp, count);
	}

	@Override
	public List<Long> getCommitTimestampsAfter(final String branch, final long timestamp, final int count) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.getBackingDB().tx(branch).getCommitTimestampsAfter(timestamp, count);
	}

	@Override
	public int countCommitTimestampsBetween(final String branch, final long from, final long to) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		return this.getBackingDB().tx(branch).countCommitTimestampsBetween(from, to);
	}

	@Override
	public int countCommitTimestamps(final String branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.getBackingDB().tx(branch).countCommitTimestamps();
	}

	@Override
	public Iterator<String> getChangedVerticesAtCommit(final String branch, final long commitTimestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		return this.getBackingDB().tx(branch).getChangedKeysAtCommit(commitTimestamp,
				ChronoGraphConstants.KEYSPACE_VERTEX);
	}

	@Override
	public Iterator<String> getChangedEdgesAtCommit(final String branch, final long commitTimestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(commitTimestamp >= 0,
				"Precondition violation - argument 'commitTimestamp' must not be negative!");
		return this.getBackingDB().tx(branch).getChangedKeysAtCommit(commitTimestamp,
				ChronoGraphConstants.KEYSPACE_EDGE);
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
		this.getBackingDB().readDump(dumpFile, options.toArray());
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
	public Features features() {
		return this.features;
	}

}
