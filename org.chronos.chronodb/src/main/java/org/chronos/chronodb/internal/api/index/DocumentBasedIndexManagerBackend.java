package org.chronos.chronodb.internal.api.index;

import java.util.Collection;
import java.util.Map;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import com.google.common.collect.SetMultimap;

/**
 * An {@link IndexManagerBackend} that uses the concept of {@linkplain ChronoIndexDocument index documents}.
 *
 * <p>
 * The basic idea of this kind of indexing comes from Apache Lucene. The document analogy was adapted for ChronoDB indexing.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface DocumentBasedIndexManagerBackend extends IndexManagerBackend {

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	/**
	 * Deletes the contents of all indices.
	 *
	 * <p>
	 * The indexers themselves are unaffected by this method.
	 */
	public void deleteAllIndexContents();

	/**
	 * Deletes the contents of the index with the given name.
	 *
	 * @param indexName
	 *            The name of the index to delete the contents for. Must not be <code>null</code>.
	 */
	public void deleteIndexContents(String indexName);

	// =================================================================================================================
	// INDEX DOCUMENT MANAGEMENT
	// =================================================================================================================

	/**
	 * Applies the given {@linkplain ChronoIndexModifications index modifications} to this index.
	 *
	 * @param indexModifications
	 *            The index modifications to apply. Must not be <code>null</code>.
	 */
	public void applyModifications(ChronoIndexModifications indexModifications);

	// =================================================================================================================
	// INDEX QUERYING
	// =================================================================================================================

	/**
	 * Returns the set of {@link ChronoIndexDocument}s that match the given search specification at the given timestamp, in the given branch.
	 *
	 * <p>
	 * This method also searches through the origin branches (recursively). It returns only the most recent documents (up to and including the given timestamp).
	 *
	 * @param timestamp
	 *            The timestamp up to which the documents should be searched. Must not be negative.
	 * @param branch
	 *            The branch in which to start the search. Origin branches will be searched as well (recursively). Must not be <code>null</code>.
	 * @param keyspace
	 *            The keyspace to search in. Must not be <code>null</code>.
	 * @param searchSpec
	 *            The search specification to fulfill. Must not be <code>null</code>.
	 *
	 * @return The set of documents that match the given search criteria. May be empty, but never <code>null</code>.
	 */
	public Collection<ChronoIndexDocument> getMatchingDocuments(long timestamp, Branch branch,
			String keyspace, SearchSpecification<?> searchSpec);

	/**
	 * Queries the indexer state to return all documents that match the given {@link ChronoIdentifier}.
	 *
	 * <p>
	 * This search does <b>not</b> include origin branches. It only searches directly on the branch indicated by the given identifier.
	 *
	 * @param chronoIdentifier
	 *            The identifier to get the index documents for. Must not be <code>null</code>.
	 * @return A mapping from indexer name to a map from indexed value to the index document that holds this value. May be empty, but never <code>null</code>.
	 */
	public Map<String, SetMultimap<Object, ChronoIndexDocument>> getMatchingBranchLocalDocuments(
			ChronoIdentifier chronoIdentifier);

}
