package org.chronos.chronodb.internal.api.index;

import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.impl.index.AbstractBackendDelegatingIndexManager;

import com.google.common.collect.SetMultimap;

/**
 * Some {@linkplain IndexManager index managers} {@linkplain AbstractBackendDelegatingIndexManager delegate calls to a backend}, which is an implementation of this interface.
 *
 * <p>
 * This class primarily exists in order to avoid excessive inheritance in {@link IndexManager} implementations.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface IndexManagerBackend {

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	/**
	 * Loads the multimap of "index name to indexers" from the storage backend.
	 *
	 * @return The multimap of "index name to indexers". May be empty, but never <code>null</code>.
	 */
	public SetMultimap<String, Indexer<?>> loadIndexersFromPersistence();

	/**
	 * Persists the given multimap of "index name to indexers" in the storage backend.
	 *
	 * @param indexNameToIndexers
	 *            The multimap to store. Must not be <code>null</code>.
	 */
	public void persistIndexers(SetMultimap<String, Indexer<?>> indexNameToIndexers);

	/**
	 * Persists the given index name and indexer.
	 *
	 * @param indexName
	 *            The index name on which the indexer operates. Must not be <code>null</code>.
	 * @param indexer
	 *            The indexer to store. Must not be <code>null</code>.
	 */
	public void persistIndexer(String indexName, Indexer<?> indexer);

	/**
	 * Deletes the index with the given name, as well as all {@link Indexer}s bound to it.
	 *
	 * @param indexName
	 *            The name of the index to delete. Must not be <code>null</code>.
	 */
	public void deleteIndexAndIndexers(String indexName);

	/**
	 * Deletes all index names, index contents and {@link Indexer}s.
	 */
	public void deleteAllIndicesAndIndexers();

	// =================================================================================================================
	// INDEX DIRTY FLAG MANAGEMENT
	// =================================================================================================================

	/**
	 * Loads the mapping of "index name to dirty flag" from the storage backend.
	 *
	 * @return The "index name to dirty flag" mapping. May be empty, but never <code>null</code>.
	 */
	public Map<String, Boolean> loadIndexStates();

	/**
	 * Persists the given "index name to dirty flag" mapping in the storage backend.
	 *
	 * @param indexNameToDirtyFlag
	 *            The map to store in the storage backend. Must not be <code>null</code>.
	 */
	public void persistIndexDirtyStates(Map<String, Boolean> indexNameToDirtyFlag);

	// =================================================================================================================
	// INDEX DOCUMENT MANAGEMENT
	// =================================================================================================================

	/**
	 * Performs a rollback to the given timestamp of all the indices in the given branches.
	 *
	 * @param branches
	 *            The names of the branches on which to perform the index rollback. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative.
	 */
	public void rollback(Set<String> branches, long timestamp);

	/**
	 * Performs a rollback to the given timestamp of all the indices in the given branches, restricted to the documents with the given keys.
	 *
	 * @param branches
	 *            The names of the branches on which to perform the index rollback. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative.
	 * @param keys
	 *            The keys to roll back. Must not be <code>null</code>.
	 */
	public void rollback(Set<String> branches, long timestamp, Set<QualifiedKey> keys);

}
