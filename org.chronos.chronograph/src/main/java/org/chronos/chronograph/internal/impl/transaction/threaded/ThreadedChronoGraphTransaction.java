package org.chronos.chronograph.internal.impl.transaction.threaded;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.transaction.StandardChronoGraphTransaction;

public class ThreadedChronoGraphTransaction extends StandardChronoGraphTransaction {

	public ThreadedChronoGraphTransaction(final ChronoGraphInternal graph,
			final ChronoDBTransaction backendTransaction) {
		super(graph, backendTransaction);
	}

	// =====================================================================================================================
	// METADATA
	// =====================================================================================================================

	@Override
	public boolean isThreadedTx() {
		return true;
	}

	@Override
	public boolean isThreadLocalTx() {
		return false;
	}

	@Override
	public boolean isOpen() {
		// the transaction is open for as long as the graph is open
		return this.getGraph().isClosed() == false;
	}

}
