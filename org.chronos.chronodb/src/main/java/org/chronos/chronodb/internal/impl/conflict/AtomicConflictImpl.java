package org.chronos.chronodb.internal.impl.conflict;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.key.ChronoIdentifier;

public class AtomicConflictImpl implements AtomicConflict {

	private final long transactionTimestamp;
	private final ChronoIdentifier sourceKey;
	private final Object sourceValue;
	private final ChronoIdentifier targetKey;
	private final Object targetValue;
	private final AncestorFetcher ancestorFetcher;

	private boolean commonAncestorLoaded = false;
	private ChronoIdentifier commonAncestorKey;
	private Object commonAncestorValue;

	public AtomicConflictImpl(final long transactionTimestamp, final ChronoIdentifier sourceKey,
			final Object sourceValue, final ChronoIdentifier targetKey, final Object targetValue,
			final AncestorFetcher ancestorFetcher) {
		checkArgument(transactionTimestamp >= 0,
				"Precondition violation - argument 'transactionTimestamp' must not be negative!");
		checkNotNull(sourceKey, "Precondition violation - argument 'sourceKey' must not be NULL!");
		checkNotNull(targetKey, "Precondition violation - argument 'targetKey' must not be NULL!");
		checkArgument(sourceKey.getKeyspace().equals(targetKey.getKeyspace()),
				"Precondition violation - arguments 'sourceKey' and 'targetKey' must specify the same keyspaces!");
		checkArgument(sourceKey.getKey().equals(targetKey.getKey()),
				"Precondition violation - arguments 'sourceKey' and 'targetKey' must specify the same keys!");
		checkNotNull(ancestorFetcher, "Precondition violation - argument 'ancestorFetcher' must not be NULL!");
		this.transactionTimestamp = transactionTimestamp;
		this.sourceKey = sourceKey;
		this.sourceValue = sourceValue;
		this.targetKey = targetKey;
		this.targetValue = targetValue;
		this.ancestorFetcher = ancestorFetcher;
	}

	@Override
	public ChronoIdentifier getSourceKey() {
		return this.sourceKey;
	}

	@Override
	public Object getSourceValue() {
		return this.sourceValue;
	}

	@Override
	public ChronoIdentifier getTargetKey() {
		return this.targetKey;
	}

	@Override
	public Object getTargetValue() {
		return this.targetValue;
	}

	@Override
	public ChronoIdentifier getCommonAncestorKey() {
		this.assertCommonAncestorIsLoaded();
		return this.commonAncestorKey;
	}

	@Override
	public Object getCommonAncestorValue() {
		this.assertCommonAncestorIsLoaded();
		return this.commonAncestorValue;
	}

	@Override
	public long getTransactionTimestamp() {
		return this.transactionTimestamp;
	}

	private void assertCommonAncestorIsLoaded() {
		if (this.commonAncestorLoaded) {
			return;
		}
		Pair<ChronoIdentifier, Object> ancestor = this.ancestorFetcher.findCommonAncestor(this.transactionTimestamp,
				this.getSourceKey(), this.getTargetKey());
		if (ancestor == null) {
			// there is no common ancestor (can this even happen?)
			this.commonAncestorKey = null;
			this.commonAncestorValue = null;
		} else {
			this.commonAncestorKey = ancestor.getKey();
			this.commonAncestorValue = ancestor.getValue();
		}
		this.commonAncestorLoaded = true;
	}

}
