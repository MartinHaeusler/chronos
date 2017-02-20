package org.chronos.chronodb.internal.api.index;

import java.util.Collection;
import java.util.Map;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.query.SearchSpecification;

public interface DocumentBasedIndexManagerBackend extends IndexManagerBackend {

	// =================================================================================================================
	// INDEXER MANAGEMENT
	// =================================================================================================================

	public void deleteIndexContents(String indexName);

	// =================================================================================================================
	// INDEX DOCUMENT MANAGEMENT
	// =================================================================================================================

	public void applyModifications(ChronoIndexModifications indexModifications);

	// =================================================================================================================
	// INDEX QUERYING
	// =================================================================================================================

	/**
	 * Returns the set of {@link ChronoIndexDocument}s that match the given search specification at the given timestamp,
	 * in the given branch.
	 *
	 * <p>
	 * This method also searches through the origin branches (recursively). It returns only the most recent documents
	 * (up to and including the given timestamp).
	 *
	 * @param timestamp
	 *            The timestamp up to which the documents should be searched. Must not be negative.
	 * @param branch
	 *            The branch in which to start the search. Origin branches will be searched as well (recursively). Must
	 *            not be <code>null</code>.
	 * @param searchSpec
	 *            The search specification to fulfill. Must not be <code>null</code>.
	 *
	 * @return The set of documents that match the given search criteria. May be empty, but never <code>null</code>.
	 */
	public Collection<ChronoIndexDocument> getMatchingDocuments(long timestamp, Branch branch,
			SearchSpecification searchSpec);

	/**
	 * Queries the indexer state to return all documents that match the given {@link ChronoIdentifier}.
	 *
	 * <p>
	 * This search does <b>not</b> include origin branches. It only searches directly on the branch indicated by the
	 * given identifier.
	 *
	 * @param chronoIdentifier
	 *            The identifier to get the index documents for. Must not be <code>null</code>.
	 * @return A mapping from indexer name to a map from indexed value to the index document that holds this value. May
	 *         be empty, but never <code>null</code>.
	 */
	public Map<String, Map<String, ChronoIndexDocument>> getMatchingBranchLocalDocuments(
			ChronoIdentifier chronoIdentifier);

}
