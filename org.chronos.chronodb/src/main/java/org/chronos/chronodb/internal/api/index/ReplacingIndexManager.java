package org.chronos.chronodb.internal.api.index;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.key.ChronoIdentifier;

public interface ReplacingIndexManager extends IndexManager {

	/**
	 * Indexes the given key-value pairs.
	 *
	 * <p>
	 * This method is <b>not</b> considered part of the public API. Clients must not call this method directly!
	 *
	 * @param identifierToOldAndNewValue
	 *            The map of changed {@link ChronoIdentifier}s to their respective old and new values to index. Must not
	 *            be <code>null</code>, may be empty.
	 */
	public void index(Map<ChronoIdentifier, Pair<Object, Object>> identifierToOldAndNewValue);
}
