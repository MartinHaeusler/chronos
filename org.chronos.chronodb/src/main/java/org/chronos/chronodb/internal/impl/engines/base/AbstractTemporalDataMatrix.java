package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;

import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.TemporalDataMatrix;
import org.chronos.chronodb.internal.util.IteratorUtils;

import com.google.common.collect.Iterators;

public abstract class AbstractTemporalDataMatrix implements TemporalDataMatrix {

	private final String keyspace;
	private final long creationTimestamp;

	protected AbstractTemporalDataMatrix(final String keyspace, final long creationTimestamp) {
		checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
		checkArgument(creationTimestamp >= 0, "Precondition violation - argument 'creationTimestamp' must not be negative!");
		this.keyspace = keyspace;
		this.creationTimestamp = creationTimestamp;
	}

	@Override
	public String getKeyspace() {
		return this.keyspace;
	}

	@Override
	public long getCreationTimestamp() {
		return this.creationTimestamp;
	}

	@Override
	public Iterator<Long> getCommitTimestampsBetween(final long timestampLowerBound, final long timestampUpperBound) {
		checkArgument(timestampLowerBound >= 0, "Precondition violation - argument 'timestampLowerBound' must not be negative!");
		checkArgument(timestampUpperBound >= 0, "Precondition violation - argument 'timestampUpperBound' must not be negative!");
		checkArgument(timestampLowerBound <= timestampUpperBound, "Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
		Iterator<TemporalKey> iterator = this.getModificationsBetween(timestampLowerBound, timestampUpperBound);
		Iterator<Long> timestamps = Iterators.transform(iterator, tk -> tk.getTimestamp());
		return IteratorUtils.unique(timestamps);
	}

}
