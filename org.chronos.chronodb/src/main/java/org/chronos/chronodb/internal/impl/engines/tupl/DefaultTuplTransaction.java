package org.chronos.chronodb.internal.impl.engines.tupl;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.cojen.tupl.Database;
import org.cojen.tupl.Transaction;

public class DefaultTuplTransaction implements TuplTransaction {

	protected Database database;
	protected Transaction backingTx;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public DefaultTuplTransaction(final Database database, final Transaction transaction) {
		checkNotNull(database, "Precondition violation - argument 'database' must not be NULL!");
		checkNotNull(transaction, "Precondition violation - argument 'transaction' must not be NULL!");
		this.database = database;
		this.backingTx = transaction;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Database getDB() {
		return this.database;
	}

	@Override
	public Transaction getRawTx() {
		return this.backingTx;
	}

}
