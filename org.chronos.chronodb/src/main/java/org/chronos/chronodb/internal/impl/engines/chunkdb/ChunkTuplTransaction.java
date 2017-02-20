package org.chronos.chronodb.internal.impl.engines.chunkdb;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.tupl.TuplTransaction;
import org.chronos.chronodb.internal.impl.tupl.WrappedTuplTransaction;

public class ChunkTuplTransaction extends WrappedTuplTransaction {

	private Period chunkPeriod;

	public ChunkTuplTransaction(final TuplTransaction innerTx, final Period chunkPeriod) {
		super(innerTx);
		checkNotNull(chunkPeriod, "Precondition violation - argument 'chunkPeriod' must not be NULL!");
		this.chunkPeriod = chunkPeriod;
	}

	public Period getChunkPeriod() {
		return this.chunkPeriod;
	}

}
