package org.chronos.chronograph.internal.impl.transaction;

import static com.google.common.base.Preconditions.*;

import java.util.Date;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException;
import org.chronos.chronodb.api.exceptions.ChronoDBTransactionException;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;

import com.google.common.collect.Maps;

public class ChronoGraphTransactionManagerImpl extends AbstractThreadLocalTransaction
		implements ChronoGraphTransactionManager {

	private final ChronoGraph chronoGraph;

	private final Map<Thread, ChronoGraphTransaction> threadToTx = Maps.newConcurrentMap();

	private final boolean allowAutoTx;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronoGraphTransactionManagerImpl(final ChronoGraph graph) {
		super(graph);
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		this.chronoGraph = graph;
		this.allowAutoTx = this.chronoGraph.getChronoGraphConfiguration().isTransactionAutoOpenEnabled();
	}

	// =====================================================================================================================
	// TRANSACTION OPEN METHODS
	// =====================================================================================================================

	@Override
	public boolean isOpen() {
		return this.threadToTx.get(Thread.currentThread()) != null;
	}

	@Override
	public void open(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		this.assertNoOpenTransactionExists();
		ChronoDB chronoDB = this.chronoGraph.getBackingDB();
		ChronoDBTransaction backendTransaction = chronoDB.tx(timestamp);
		ChronoGraphTransaction transaction = new StandardChronoGraphTransaction(this.chronoGraph, backendTransaction);
		this.threadToTx.put(Thread.currentThread(), transaction);
	}

	@Override
	public void open(final Date date) {
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		this.open(date.getTime());
	}

	@Override
	public void open(final String branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		this.assertNoOpenTransactionExists();
		ChronoDB chronoDB = this.chronoGraph.getBackingDB();
		ChronoDBTransaction backendTransaction = chronoDB.tx(branch);
		ChronoGraphTransaction transaction = new StandardChronoGraphTransaction(this.chronoGraph, backendTransaction);
		this.threadToTx.put(Thread.currentThread(), transaction);
	}

	@Override
	public void open(final String branch, final Date date) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		this.open(branch, date.getTime());
	}

	@Override
	public void open(final String branch, final long timestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		this.assertNoOpenTransactionExists();
		ChronoDB chronoDB = this.chronoGraph.getBackingDB();
		ChronoDBTransaction backendTransaction = chronoDB.tx(branch, timestamp);
		ChronoGraphTransaction transaction = new StandardChronoGraphTransaction(this.chronoGraph, backendTransaction);
		this.threadToTx.put(Thread.currentThread(), transaction);
	}

	@Override
	protected void doOpen() {
		this.assertNoOpenTransactionExists();
		ChronoDB chronoDB = this.chronoGraph.getBackingDB();
		ChronoDBTransaction backendTransaction = chronoDB.tx();
		ChronoGraphTransaction transaction = new StandardChronoGraphTransaction(this.chronoGraph, backendTransaction);
		this.threadToTx.put(Thread.currentThread(), transaction);
	}

	@Override
	protected void doReadWrite() {
		if (this.allowAutoTx) {
			super.doReadWrite();
		} else {
			Transaction.READ_WRITE_BEHAVIOR.MANUAL.accept(this);
		}
	}

	@Override
	public void reset() {
		// make sure that a transaction exists (tx auto), or an exception is thrown if none is open (tx strict)
		this.readWrite();
		String branchName = this.getCurrentTransaction().getBranchName();
		this.doReset(branchName, null);
	}

	@Override
	public void reset(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// make sure that a transaction exists (tx auto), or an exception is thrown if none is open (tx strict)
		this.readWrite();
		String branchName = this.getCurrentTransaction().getBranchName();
		this.doReset(branchName, timestamp);
	}

	@Override
	public void reset(final Date date) {
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		this.reset(date.getTime());
	}

	@Override
	public void reset(final String branch, final long timestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// make sure that a transaction exists (tx auto), or an exception is thrown if none is open (tx strict)
		this.readWrite();
		this.doReset(branch, timestamp);
	}

	@Override
	public void reset(final String branch, final Date date) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		this.reset(branch, date.getTime());
	}

	protected void doReset(final String branch, final Long timestamp) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		// assert that target branch exists
		boolean branchExists = this.chronoGraph.getBackingDB().getBranchManager().existsBranch(branch);
		if (branchExists == false) {
			throw new ChronoDBBranchingException("There is no branch named '" + branch + "'!");
		}
		// branch exists, perform the rollback on the current transaction
		this.rollback();
		// decide whether to jump to head revision or to a specific version
		if (timestamp == null) {
			// jump to head revision
			this.open(branch);
		} else {
			// jump to given revision
			this.open(branch, timestamp);
		}
	}

	// =====================================================================================================================
	// THREADED TX METHODS
	// =====================================================================================================================

	@Override
	@SuppressWarnings("unchecked")
	public ChronoGraph createThreadedTx() {
		return new ChronoThreadedTransactionGraph(this.chronoGraph, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	@Override
	public ChronoGraph createThreadedTx(final long timestamp) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return new ChronoThreadedTransactionGraph(this.chronoGraph, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
				timestamp);
	}

	@Override
	public ChronoGraph createThreadedTx(final Date date) {
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		return this.createThreadedTx(date.getTime());
	}

	@Override
	public ChronoGraph createThreadedTx(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		return new ChronoThreadedTransactionGraph(this.chronoGraph, branchName);
	}

	@Override
	public ChronoGraph createThreadedTx(final String branchName, final long timestamp) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		return new ChronoThreadedTransactionGraph(this.chronoGraph, branchName, timestamp);
	}

	@Override
	public ChronoGraph createThreadedTx(final String branchName, final Date date) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkNotNull(date, "Precondition violation - argument 'date' must not be NULL!");
		return this.createThreadedTx(branchName, date.getTime());
	}

	// =====================================================================================================================
	// COMMIT & ROLLBACK
	// =====================================================================================================================

	@Override
	protected void doCommit() throws TransactionException {
		ChronoGraphTransaction transaction = this.threadToTx.get(Thread.currentThread());
		if (transaction == null) {
			throw new TransactionException("No Graph Transaction is open on this thread; cannot perform commit!");
		}
		try {
			transaction.commit();
		} catch (ChronoDBTransactionException e) {
			throw new TransactionException("Commit failed due to exception in ChronoDB: " + e.toString(), e);
		} finally {
			this.threadToTx.remove(Thread.currentThread());
		}
	}

	@Override
	public void commit(final Object metadata) {
		ChronoGraphTransaction transaction = this.threadToTx.get(Thread.currentThread());
		if (transaction == null) {
			throw new RuntimeException("No Graph Transaction is open on this thread; cannot perform commit!");
		}
		try {
			transaction.commit(metadata);
		} catch (ChronoDBTransactionException e) {
			throw new RuntimeException("Commit failed due to exception in ChronoDB: " + e.toString(), e);
		} finally {
			this.threadToTx.remove(Thread.currentThread());
		}
	}

	@Override
	public void commitIncremental() {
		ChronoGraphTransaction transaction = this.threadToTx.get(Thread.currentThread());
		if (transaction == null) {
			throw new IllegalStateException("No Graph Transaction is open on this thread; cannot perform commit!");
		}
		try {
			transaction.commitIncremental();
		} catch (ChronoDBTransactionException e) {
			throw new IllegalStateException("Commit failed due to exception in ChronoDB: " + e.toString(), e);
		}
	}

	@Override
	protected void doRollback() throws TransactionException {
		ChronoGraphTransaction transaction = this.threadToTx.get(Thread.currentThread());
		if (transaction == null) {
			throw new TransactionException("No Graph Transaction is open on this thread; cannot perform rollback!");
		}
		this.threadToTx.remove(Thread.currentThread());
		try {
			transaction.rollback();
		} catch (ChronoDBTransactionException e) {
			throw new TransactionException("Rollback failed due to exception in ChronoDB: " + e.toString(), e);
		}
	}

	@Override
	public ChronoGraphTransaction getCurrentTransaction() {
		ChronoGraphTransaction transaction = this.threadToTx.get(Thread.currentThread());
		if (transaction == null) {
			throw Transaction.Exceptions.transactionMustBeOpenToReadWrite();
		}
		return transaction;
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private void assertNoOpenTransactionExists() {
		if (this.isOpen()) {
			throw Transaction.Exceptions.transactionAlreadyOpen();
		}
	}

}
