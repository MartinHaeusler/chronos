package org.chronos.chronograph.internal.api.index;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronograph.api.index.ChronoGraphIndex;

/**
 * This class is the internal representation of {@link ChronoGraphIndex}, offering additional methods to be used by internal API only.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphIndexInternal extends ChronoGraphIndex {

	/**
	 * Returns the index key used in the backing {@link ChronoDB} index.
	 *
	 * @return The backend index key. Never <code>null</code>.
	 */
	public String getBackendIndexKey();

	/**
	 * Creates a new {@link Indexer} instance that corresponds to the configuration of this index.
	 *
	 * @return The newly created indexer. Never <code>null</code>.
	 */
	public Indexer<?> createIndexer();

}
