package org.chronos.chronodb.internal.impl.tupl;

import static com.google.common.base.Preconditions.*;

import org.cojen.tupl.Database;
import org.cojen.tupl.Transaction;

public abstract class WrappedTuplTransaction implements TuplTransaction {

	protected final TuplTransaction innerTransaction;

	protected WrappedTuplTransaction(final TuplTransaction innerTransaction) {
		checkNotNull(innerTransaction, "Precondition violation - argument 'innerTransaction' must not be NULL!");
		this.innerTransaction = innerTransaction;
	}

	@Override
	public void close() {
		this.innerTransaction.close();
	}

	@Override
	public void commit() {
		this.innerTransaction.commit();
	}

	@Override
	public void rollback() {
		this.innerTransaction.rollback();
	}

	@Override
	public Database getDB() {
		return this.innerTransaction.getDB();
	}

	@Override
	public Transaction getRawTx() {
		return this.innerTransaction.getRawTx();
	}

}
