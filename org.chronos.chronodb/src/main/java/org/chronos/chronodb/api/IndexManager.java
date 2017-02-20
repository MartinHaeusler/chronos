package org.chronos.chronodb.api;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.chronos.chronodb.api.exceptions.IndexerConflictException;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.index.DocumentBasedIndexManagerBackend;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.api.query.SearchSpecification;
import org.chronos.chronodb.internal.impl.index.DocumentBasedIndexManager;

/**
 * An {@link IndexManager} is a backend-agnostic facade for indexing in a {@link ChronoDB}.
 *
 * <p>
 * The responsibilities of an index manager include:
 * <ul>
 * <li>Management of registered {@link ChronoIndexer} instances
 * <li>Performing indexing and re-indexing of the database
 * <li>Evaluating {@link ChronoDBQuery} instances on the index
 * </ul>
 *
 * <p>
 * Instances of this interface can be retrieved via {@link ChronoDB#getIndexManager()}. API users should not attempt to
 * create an instance of a class that implements this interface manually.
 *
 * <p>
 * <b>Implementation notes</b>
 *
 * <p>
 * While an index manager implementation may be implemented specifically for a given backend, implementations are
 * usually generic and the actual indexing backend is provided via an instance of
 * {@link DocumentBasedIndexManagerBackend}. One example of such an approach is the {@link DocumentBasedIndexManager}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface IndexManager {

	// =================================================================================================================
	// INDEX MANAGEMENT
	// =================================================================================================================

	/**
	 * Returns the names of all known indices in an immutable set.
	 *
	 * @return An immutable view on all known index names.
	 */
	public Set<String> getIndexNames();

	/**
	 * Removes the index with the given name, including index structures and registered indexers.
	 *
	 * <p>
	 * If the index with the given name does not exist, this method does nothing.
	 *
	 * <p>
	 * If a specific indexer should be removed, please consider using {@link #removeIndexer(ChronoIndexer)} instead.
	 *
	 *
	 * @param indexName
	 *            The name of the index to remove. Must not be <code>null</code>.
	 */
	public void removeIndex(String indexName);

	/**
	 * Clears all index information on all known indices and also removes all {@link ChronoIndexer}s.
	 *
	 * <p>
	 * This method effectively <b>resets</b> the index to an empty state and should be used with great care.
	 *
	 * <p>
	 * <b>WARNING</b>: This operation cannot be undone!
	 */
	public void clearAllIndices();

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	/**
	 * Adds the given indexer to the index with the given name.
	 *
	 * <p>
	 * Please note that for any index, there must be <b>at most one</b> indexer that accepts any given object. If there
	 * are multiple indexers on the same index that accept a given object upon insertion, a
	 * {@link IndexerConflictException} is thrown when the object is inserted.
	 *
	 * <p>
	 * Adding an indexer dirties the index. Consider calling {@link #reindexAll()} when all changes to the indexers are
	 * complete.
	 *
	 * @param indexName
	 *            The name of the index to which the indexer should be added. Must not be <code>null</code>. If there is
	 *            no index with the given name yet, it is created on-the-fly.
	 * @param indexer
	 *            The indexer to add. Must not be <code>null</code>.
	 */
	public void addIndexer(String indexName, ChronoIndexer indexer);

	/**
	 * Removes the given indexer from all indices where it is used.
	 *
	 * <p>
	 * If the indexer is not in use, this method does nothing.
	 *
	 * <p>
	 * Removing an indexer dirties the index. Consider calling {@link #reindexAll()} when all changes to the indexers
	 * are complete.
	 *
	 * @param indexer
	 *            The indexer to remove. Must not be <code>null</code>.
	 */
	public void removeIndexer(ChronoIndexer indexer);

	/**
	 * Returns an unmodifiable view on the indexers which are currently in use.
	 *
	 * @return An unmodifiable view on the indexers. May be empty, but never <code>null</code>.
	 */
	public Set<ChronoIndexer> getIndexers();

	/**
	 * Returns a mapping from index name to the indexers which are in use by that index.
	 *
	 * @return An unmodifiable mapping from index name to indexers. May be empty, but never <code>null</code>.
	 */
	public Map<String, Set<ChronoIndexer>> getIndexersByIndexName();

	// =================================================================================================================
	// INDEXING METHODS
	// =================================================================================================================

	/**
	 * Re-indexes the index with the given name.
	 *
	 * <p>
	 * This operation will <b>force</b> a re-index. Re-indexing is an expensive operation. Furthermore, re-indexing is
	 * an exclusive operation that does not permit any concurrent reads and/or writes on the database.
	 *
	 * <p>
	 * If you are unsure if any index is dirty and requires re-indexing, please consider using {@link #reindexAll()}
	 * instead.
	 *
	 * @param indexName
	 *            The name of the index to re-index. Must not be <code>null</code>.
	 */
	public void reindex(String indexName);

	/**
	 * Re-indexes all dirty indices.
	 *
	 * <p>
	 * Note that this is not a forced re-index. Only dirty indices will be re-indexed.
	 *
	 * <p>
	 * Re-indexing is an expensive operation. Furthermore, re-indexing is an exclusive operation that does not permit
	 * any concurrent reads and/or writes on the database.
	 */
	public void reindexAll();

	/**
	 * Checks if any index is dirty and requires re-indexing.
	 *
	 * <p>
	 * If this method returns <code>true</code>, please consider calling {@link #reindexAll()} to create a clean index.
	 *
	 * @return <code>true</code> if at least one index is dirty and requires re-indexing, otherwise <code>false</code>.
	 */
	public boolean isReindexingRequired();

	/**
	 * Returns an immutable set of all index names that are currently referring to dirty indices.
	 *
	 * @return The immutable set of the names of dirty indices. May be empty, but never <code>null</code>.
	 */
	public Set<String> getDirtyIndices();

	// =================================================================================================================
	// INDEX QUERY METHODS
	// =================================================================================================================

	/**
	 * Queries the index by providing a value description.
	 *
	 * @param timestamp
	 *            The timestamp at which the query takes place. Must not be negative.
	 * @param branch
	 *            The branch to evaluate the query in. Must not be <code>null</code>. Must refer to an existing branch.
	 * @param searchSpec
	 *            The search specification to fulfill. Must not be <code>null</code>.
	 *
	 * @return The set of identifiers that have a value assigned that matches the given description. May be empty, but
	 *         never <code>null</code>.
	 */
	public Set<ChronoIdentifier> queryIndex(final long timestamp, Branch branch, SearchSpecification searchSpec);

	/**
	 * Evaluates the given {@link ChronoDBQuery}.
	 *
	 * @param timestamp
	 *            The timestamp at which the evaluation takes place. Must not be negative.
	 * @param branch
	 *            The branch to evaluate the query in. Must not be <code>null</code>.
	 * @param query
	 *            The query to run. Must not be <code>null</code>. Must have been optimized before calling this method.
	 *
	 * @return An iterator on the keys that have values assigned which match the query. May be empty, but never
	 *         <code>null</code>.
	 */
	public Iterator<QualifiedKey> evaluate(long timestamp, Branch branch, ChronoDBQuery query);

	/**
	 * Evaluates the given {@link ChronoDBQuery}.
	 *
	 * @param timestamp
	 *            The timestamp at which the evaluation takes place. Must not be negative.
	 * @param branch
	 *            The branch to evaluate the query in. Must not be <code>null</code>.
	 * @param query
	 *            The query to run. Must not be <code>null</code>. Must have been optimized before calling this method.
	 *
	 * @return The number of key-value pairs in the database that match the given query. May be zero, but never
	 *         negative.
	 */
	public long evaluateCount(long timestamp, Branch branch, ChronoDBQuery query);

	// =====================================================================================================================
	// ROLLBACK METHODS
	// =====================================================================================================================

	/**
	 * Performs a rollback on the index to the given timestamp.
	 *
	 * <p>
	 * This affects all branches. Any index entries that belong to later timestamps will be removed.
	 *
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative.
	 *
	 * @see #rollback(Branch, long)
	 * @see #rollback(Branch, long, Set)
	 */
	public void rollback(long timestamp);

	/**
	 * Performs a rollback on the index in the given branch to the given timestamp, for the given set of keys only.
	 *
	 * <p>
	 * All index entries that are unrelated to the given keys will remain untouched by this operation, even if these
	 * entries were added and/or modified after the given timestamp.
	 *
	 * @param branch
	 *            The branch to roll back. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative.
	 * @param keys
	 *            The keys to roll back. Must not be <code>null</code>. If this set is empty, this method returns
	 *            immediately and has no effect.
	 *
	 * @see #rollback(long)
	 */
	public void rollback(Branch branch, long timestamp, Set<QualifiedKey> keys);

	/**
	 * Performs a rollback on the index in the given branch to the given timestamp.
	 *
	 * <p>
	 * This operation will affect all entries in the given branch after the given timestamp.
	 *
	 * @param branch
	 *            The branch to roll back. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative.
	 *
	 * @see #rollback(long)
	 * @see #rollback(Branch, long, Set)
	 */
	public void rollback(Branch branch, long timestamp);

	/**
	 * Clears the internal query cache, if query result caching is enabled.
	 * <p>
	 * If query result caching is disabled, this method does nothing.
	 */
	public void clearQueryCache();

}
