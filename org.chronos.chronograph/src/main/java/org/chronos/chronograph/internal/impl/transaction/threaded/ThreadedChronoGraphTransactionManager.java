package org.chronos.chronograph.internal.impl.transaction.threaded;

import java.util.Date;

import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadedTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;

public class ThreadedChronoGraphTransactionManager extends AbstractThreadedTransaction
		implements ChronoGraphTransactionManager {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final ChronoThreadedTransactionGraph owningGraph;
	private final ThreadedChronoGraphTransaction tx;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ThreadedChronoGraphTransactionManager(final ChronoThreadedTransactionGraph g,
			final ThreadedChronoGraphTransaction tx) {
		super(g);
		this.owningGraph = g;
		this.tx = tx;
	}

	// =====================================================================================================================
	// TRANSACTION OPENING
	// =====================================================================================================================

	@Override
	public boolean isOpen() {
		// this transaction is open for as long as the owning graph is open
		return this.owningGraph.isClosed() == false;
	}

	@Override
	public void open(final long timestamp) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public void open(final Date date) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public void open(final String branch) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public void open(final String branch, final long timestamp) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public void open(final String branch, final Date date) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public void reset() {
		throw new UnsupportedOperationException("Cannot reset transactions from inside a Threaded Transaction!");
	}

	@Override
	public void reset(final long timestamp) {
		throw new UnsupportedOperationException("Cannot reset transactions from inside a Threaded Transaction!");
	}

	@Override
	public void reset(final Date date) {
		throw new UnsupportedOperationException("Cannot reset transactions from inside a Threaded Transaction!");
	}

	@Override
	public void reset(final String branch, final long timestamp) {
		throw new UnsupportedOperationException("Cannot reset transactions from inside a Threaded Transaction!");
	}

	@Override
	public void reset(final String branch, final Date date) {
		throw new UnsupportedOperationException("Cannot reset transactions from inside a Threaded Transaction!");
	}

	// =====================================================================================================================
	// THREADED TX METHODS
	// =====================================================================================================================

	@Override
	@SuppressWarnings("unchecked")
	public ChronoGraph createThreadedTx() {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public ChronoGraph createThreadedTx(final long timestamp) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public ChronoGraph createThreadedTx(final Date date) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public ChronoGraph createThreadedTx(final String branchName) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public ChronoGraph createThreadedTx(final String branchName, final long timestamp) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	public ChronoGraph createThreadedTx(final String branchName, final Date date) {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	// =====================================================================================================================
	// CURRENT TRANSACTION
	// =====================================================================================================================

	@Override
	public ChronoGraphTransaction getCurrentTransaction() {
		return this.tx;
	}

	// =====================================================================================================================
	// OPENING & CLOSING
	// =====================================================================================================================

	@Override
	protected void doOpen() {
		throw new UnsupportedOperationException("Cannot open transactions from inside a Threaded Transaction!");
	}

	@Override
	protected void doCommit() throws TransactionException {
		this.tx.commit();
	}

	@Override
	public void commit(final Object metadata) {
		this.tx.commit(metadata);
	}

	@Override
	public void commitIncremental() {
		this.tx.commitIncremental();
	}

	@Override
	protected void doRollback() throws TransactionException {
		this.tx.rollback();
		this.owningGraph.close();
	}

}
