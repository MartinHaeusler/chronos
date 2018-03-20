package org.chronos.chronodb.internal.impl.conflict;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.key.ChronoIdentifier;

public interface AncestorFetcher {

	public Pair<ChronoIdentifier, Object> findCommonAncestor(long transactionTimestmp, ChronoIdentifier source,
			ChronoIdentifier target);

}
