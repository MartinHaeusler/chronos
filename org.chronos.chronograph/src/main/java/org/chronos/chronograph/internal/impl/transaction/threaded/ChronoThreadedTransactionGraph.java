package org.chronos.chronograph.internal.impl.transaction.threaded;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.Order;
import org.chronos.chronograph.api.branch.ChronoGraphBranchManager;
import org.chronos.chronograph.api.builder.query.GraphQueryBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.internal.impl.builder.query.GraphQueryBuilderStarterImpl;

import com.google.common.collect.Maps;

public class ChronoThreadedTransactionGraph implements ChronoGraph {

	private final ChronoGraph originalGraph;

	private final ChronoGraphTransactionManager txManager;
	private final Map<String, ChronoGraphIndexManager> branchNameToIndexManager;

	private boolean isClosed;

	public ChronoThreadedTransactionGraph(final ChronoGraph originalGraph, final String branchName) {
		this(originalGraph, branchName, null);
	}

	public ChronoThreadedTransactionGraph(final ChronoGraph originalGraph, final String branchName,
			final long timestamp) {
		this(originalGraph, branchName, Long.valueOf(timestamp));
	}

	public ChronoThreadedTransactionGraph(final ChronoGraph originalGraph, final String branchName,
			final Long timestamp) {
		checkNotNull(originalGraph, "Precondition violation - argument 'originalGraph' must not be NULL!");
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		if (timestamp != null) {
			checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		}
		this.originalGraph = originalGraph;
		this.isClosed = false;
		// initialize the graph transaction which is bound to this graph
		ChronoDB db = this.originalGraph.getBackingDB();
		ChronoDBTransaction backendTransaction = null;
		if (timestamp == null) {
			// no timestamp given, use head revision
			backendTransaction = db.tx(branchName);
		} else {
			// timestamp given, use the given revision
			backendTransaction = db.tx(branchName, timestamp);
		}
		ThreadedChronoGraphTransaction graphTx = new ThreadedChronoGraphTransaction(this, backendTransaction);
		// build the "pseudo transaction manager" that only returns the just created graph transaction
		this.txManager = new ThreadedChronoGraphTransactionManager(this, graphTx);
		this.branchNameToIndexManager = Maps.newHashMap();
	}

	// =================================================================================================================
	// GRAPH CLOSING
	// =================================================================================================================

	@Override
	public void close() {
		if (this.isClosed) {
			return;
		}
		// close the singleton transaction
		if (this.tx().isOpen()) {
			this.tx().close();
		}
		this.isClosed = true;
	}

	@Override
	public boolean isClosed() {
		return this.isClosed || this.originalGraph.isClosed();
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
		ChronoGraphTransaction transaction = this.tx().getCurrentTransaction();
		if (transaction.isOpen() == false) {
			throw new IllegalStateException("This threaded transaction was already closed!");
		}
		return this.getBackingDB().getBranchManager().getMasterBranch().getNow();
	}

	@Override
	public long getNow(final String branchName) {
		ChronoGraphTransaction transaction = this.tx().getCurrentTransaction();
		if (transaction.isOpen() == false) {
			throw new IllegalStateException("This threaded transaction was already closed!");
		}
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
		if (this.getBackingDB().getBranchManager().existsBranch(branchName) == false) {
			throw new IllegalArgumentException("There is no branch named '" + branchName + "'!");
		}
		// try to retrieve a cached copy of the manager
		ChronoGraphIndexManager indexManager = this.branchNameToIndexManager.get(branchName);
		if (indexManager == null) {
			// manager not present in our cache; build it and add it to the cache
			indexManager = new ThreadedChronoGraphIndexManager(this, branchName);
			this.branchNameToIndexManager.put(branchName, indexManager);
		}
		return indexManager;
	}

	// =====================================================================================================================
	// BRANCHING
	// =====================================================================================================================

	@Override
	public ChronoGraphBranchManager getBranchManager() {
		return this.originalGraph.getBranchManager();
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
		throw new IllegalStateException("Graph variables are not accessible in a Threaded Transaction!");
	}

	@Override
	public Configuration configuration() {
		return this.originalGraph.configuration();
	}

	@Override
	public ChronoGraphConfiguration getChronoGraphConfiguration() {
		return this.originalGraph.getChronoGraphConfiguration();
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
		return this.originalGraph.getCommitMetadata(branch, timestamp);
	}

	@Override
	public Iterator<Long> getCommitTimestampsBetween(final String branch, final long from, final long to,
			final Order order) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		return this.originalGraph.getCommitTimestampsBetween(branch, from, to);
	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final String branch, final long from, final long to,
			final Order order) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		return this.originalGraph.getCommitMetadataBetween(branch, from, to, order);
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
		return this.originalGraph.getCommitTimestampsPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex,
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
		return this.originalGraph.getCommitMetadataPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex,
				order);
	}

	@Override
	public int countCommitTimestampsBetween(final String branch, final long from, final long to) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		return this.originalGraph.countCommitTimestampsBetween(branch, from, to);
	}

	@Override
	public int countCommitTimestamps(final String branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.originalGraph.countCommitTimestamps(branch);
	}

	// =====================================================================================================================
	// SERIALIZATION & DESERIALIZATION (GraphSon, Gyro, ...)
	// =====================================================================================================================

	@Override
	@SuppressWarnings({ "rawtypes" })
	public <I extends Io> I io(final Builder<I> builder) {
		return this.originalGraph.io(builder);
	}

	// =====================================================================================================================
	// DUMP OPERATIONS
	// =====================================================================================================================

	@Override
	public void writeDump(final File dumpFile, final DumpOption... dumpOptions) {
		throw new UnsupportedOperationException("createDump(...) is not permitted on threaded transaction graphs. "
				+ "Call it on the original graph instead.");
	}

	@Override
	public void readDump(final File dumpFile, final DumpOption... options) {
		throw new UnsupportedOperationException("readDump(...) is not permitted on threaded transaction graphs. "
				+ "Call it on the original graph instance.");
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
		return this.originalGraph.getBackingDB();
	}

	public ChronoGraph getOriginalGraph() {
		return this.originalGraph;
	}

	// =====================================================================================================================
	// FEATURES DECLARATION
	// =====================================================================================================================

	@Override
	public Features features() {
		return this.originalGraph.features();
	}

}
