package org.chronos.chronodb.api.conflict;

import org.chronos.chronodb.api.key.ChronoIdentifier;

public interface AtomicConflict {

	public long getTransactionTimestamp();

	public ChronoIdentifier getSourceKey();

	public Object getSourceValue();

	public ChronoIdentifier getTargetKey();

	public Object getTargetValue();

	public ChronoIdentifier getCommonAncestorKey();

	public Object getCommonAncestorValue();

}
