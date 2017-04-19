package org.chronos.chronosphere.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereBranchManager;
import org.chronos.chronosphere.api.ChronoSphereEPackageManager;
import org.chronos.chronosphere.api.ChronoSphereIndexManager;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.SphereBranch;
import org.chronos.chronosphere.impl.transaction.ChronoSphereTransactionImpl;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.configuration.api.ChronoSphereConfiguration;
import org.chronos.chronosphere.internal.configuration.impl.ChronoSphereConfigurationImpl;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.EObjectToGraphMapper;
import org.chronos.chronosphere.internal.ogm.api.EPackageToGraphMapper;
import org.chronos.chronosphere.internal.ogm.impl.EObjectToGraphMapperImpl;
import org.chronos.chronosphere.internal.ogm.impl.EPackageToGraphMapperImpl;
import org.chronos.common.configuration.ChronosConfigurationUtil;
import org.eclipse.emf.ecore.EObject;

import com.google.common.collect.Maps;

public class StandardChronoSphere implements ChronoSphere, ChronoSphereInternal {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final ChronoGraph graph;
	private final ChronoSphereConfiguration configuration;
	private final ChronoSphereBranchManager branchManager;
	private final ChronoSphereEPackageManager ePackageManager;

	private final EObjectToGraphMapper eObjectToGraphMapper;
	private final EPackageToGraphMapper ePackageToGraphMapper;

	private final Lock branchLock;
	private final Map<SphereBranch, ChronoSphereIndexManager> branchToIndexManager;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public StandardChronoSphere(final ChronoGraph graph, final Configuration configuration) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		this.graph = graph;
		this.configuration = ChronosConfigurationUtil.build(configuration, ChronoSphereConfigurationImpl.class);
		this.eObjectToGraphMapper = new EObjectToGraphMapperImpl();
		this.ePackageToGraphMapper = new EPackageToGraphMapperImpl();
		this.branchManager = new ChronoSphereBranchManagerImpl(graph);
		this.ePackageManager = new ChronoSphereEPackageManagerImpl(this);
		this.branchLock = new ReentrantLock(true);
		this.branchToIndexManager = Maps.newHashMap();
		// make sure that the graph has the default indices registered
		this.setUpDefaultGraphIndicesIfNecessary();
	}

	// =====================================================================================================================
	// [PUBLIC API] CLOSE HANDLING
	// =====================================================================================================================

	@Override
	public ChronoSphereConfiguration getConfiguration() {
		return this.configuration;
	}

	@Override
	public void close() {
		this.graph.close();
	}

	@Override
	public boolean isClosed() {
		return this.graph.isClosed();
	}

	// =====================================================================================================================
	// [PUBLIC API] BRANCH MANAGEMENT
	// =====================================================================================================================

	@Override
	public ChronoSphereBranchManager getBranchManager() {
		return this.branchManager;
	}

	// =================================================================================================================
	// [PUBLIC API] EPACKAGE MANAGEMENT
	// =================================================================================================================

	@Override
	public ChronoSphereEPackageManager getEPackageManager() {
		return this.ePackageManager;
	}

	// =====================================================================================================================
	// [PUBLIC API] INDEX MANAGEMENT
	// =====================================================================================================================

	@Override
	public ChronoSphereIndexManager getIndexManager(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		SphereBranch branch = this.getBranchManager().getBranch(branchName);
		this.branchLock.lock();
		try {
			ChronoSphereIndexManager indexManager = this.branchToIndexManager.get(branch);
			if (indexManager != null) {
				// index manager already exists
				return indexManager;
			} else {
				// no index manager exists yet; create one
				indexManager = new ChronoSphereIndexManagerImpl(this, branch);
				this.branchToIndexManager.put(branch, indexManager);
				return indexManager;
			}

		} finally {
			this.branchLock.unlock();
		}
	}

	// =====================================================================================================================
	// [PUBLIC API] TRANSACTION HANDLING
	// =====================================================================================================================

	@Override
	public ChronoSphereTransaction tx() {
		ChronoGraph txGraph = this.getRootGraph().tx().createThreadedTx();
		return new ChronoSphereTransactionImpl(this, txGraph);
	}

	@Override
	public ChronoSphereTransaction tx(final long timestamp) throws InvalidTransactionTimestampException {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		ChronoGraph txGraph = this.getRootGraph().tx().createThreadedTx(timestamp);
		return new ChronoSphereTransactionImpl(this, txGraph);
	}

	@Override
	public ChronoSphereTransaction tx(final Date date) throws InvalidTransactionTimestampException {
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		return this.tx(date.getTime());
	}

	@Override
	public ChronoSphereTransaction tx(final String branchName) throws InvalidTransactionBranchException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		ChronoGraph txGraph = this.getRootGraph().tx().createThreadedTx(branchName);
		return new ChronoSphereTransactionImpl(this, txGraph);
	}

	@Override
	public ChronoSphereTransaction tx(final String branchName, final long timestamp)
			throws InvalidTransactionBranchException, InvalidTransactionTimestampException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		ChronoGraph txGraph = this.getRootGraph().tx().createThreadedTx(branchName, timestamp);
		return new ChronoSphereTransactionImpl(this, txGraph);
	}

	@Override
	public ChronoSphereTransaction tx(final String branchName, final Date date)
			throws InvalidTransactionBranchException, InvalidTransactionTimestampException {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		return this.tx(branchName, date.getTime());
	}

	// =====================================================================================================================
	// [PUBLIC API] BATCH INSERTION
	// =====================================================================================================================

	@Override
	public void batchInsertModelData(final String branch, final Iterator<EObject> model) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(this.getBranchManager().existsBranch(branch),
				"Precondition violation - argument 'branch' must refer to an existing branch!");
		checkNotNull(model, "Precondition violation - argument 'model' must not be NULL!");
		try (ChronoSphereTransactionInternal tx = (ChronoSphereTransactionInternal) this.tx(branch)) {
			tx.commitIncremental();
			tx.batchInsert(model);
			tx.commit();
		}
	}

	// =================================================================================================================
	// [PUBLIC API] HISTORY ANALYSIS
	// =================================================================================================================

	@Override
	public long getNow(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		return this.graph.getNow(branchName);
	}

	@Override
	public Object getCommitMetadata(final String branch, final long timestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkArgument(timestamp <= this.getNow(branch),
				"Precondition violation - argument 'timestamp' must not be larger than the latest commit timestamp!");
		return this.graph.getCommitMetadata(branch, timestamp);
	}

	@Override
	public Iterator<Long> getCommitTimestampsBetween(final String branch, final long from, final long to,
			final Order order) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		return this.graph.getCommitTimestampsBetween(branch, from, to, order);
	}

	@Override
	public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final String branch, final long from, final long to,
			final Order order) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		return this.graph.getCommitMetadataBetween(branch, from, to, order);
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
		return this.graph.getCommitTimestampsPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex, order);
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
		return this.graph.getCommitMetadataPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex, order);
	}

	@Override
	public int countCommitTimestampsBetween(final String branch, final long from, final long to) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
		checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
		return this.graph.countCommitTimestampsBetween(branch, from, to);
	}

	@Override
	public int countCommitTimestamps(final String branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		return this.graph.countCommitTimestamps(branch);
	}

	// =====================================================================================================================
	// INTERNAL API
	// =====================================================================================================================

	@Override
	public ChronoGraph getRootGraph() {
		return this.graph;
	}

	@Override
	public EObjectToGraphMapper getEObjectToGraphMapper() {
		return this.eObjectToGraphMapper;
	}

	@Override
	public EPackageToGraphMapper getEPackageToGraphMapper() {
		return this.ePackageToGraphMapper;
	}

	// =====================================================================================================================
	// HELPER METHODS
	// =====================================================================================================================

	private void setUpDefaultGraphIndicesIfNecessary() {
		ChronoGraphIndexManager indexManager = this.getRootGraph().getIndexManager();
		ChronoGraphIndex kindIndex = indexManager.getVertexIndex(ChronoSphereGraphFormat.V_PROP__KIND);
		if (kindIndex == null) {
			indexManager.createIndex().onVertexProperty(ChronoSphereGraphFormat.V_PROP__KIND).build();
		}
	}

}
