package org.chronos.chronodb.internal.api.index;

import java.util.Map;

import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.key.ChronoIdentifier;

public interface AppendingIndexManager extends IndexManager {

	/**
	 * Indexes the given key-value pairs.
	 *
	 * <p>
	 * This method is <b>not</b> considered part of the public API. Clients must not call this method directly!
	 *
	 * @param identifierToNewValue
	 *            The map of changed {@link ChronoIdentifier}s to their respective new value to index. Must not be
	 *            <code>null</code>, may be empty.
	 */
	public void index(Map<ChronoIdentifier, Object> identifierToNewValue);
}
