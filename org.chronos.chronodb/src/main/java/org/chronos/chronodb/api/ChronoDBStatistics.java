package org.chronos.chronodb.api;

import java.util.Set;

public interface ChronoDBStatistics {

	// =================================================================================================================
	// BRANCHING DATA
	// =================================================================================================================

	/**
	 * Returns an immutable set, containing all branch names.
	 *
	 * @return The immutable set of all branch names. Never <code>null</code>. Will always at least contain the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
	 */
	public Set<String> getBranchNames();

	/**
	 * Returns the immutable set of known keyspace names in the given branch.
	 *
	 * @param branchName
	 *            The name of the branch to retrieve the keyspaces for. Must not be <code>null</code>.
	 * @return The immutable set of known keyspace names in the given branch. May be empty, but never <code>null</code>. Is guaranteed to be empty for non-existing branches.
	 */
	public Set<String> getKeyspacesInBranch(String branchName);

	/**
	 * Returns the number of branches.
	 *
	 * @return The number of branches. Guaranteed to be greater than or equal to one (the master branch).
	 */
	public int getNumberOfBranches();

	/**
	 * Returns the maximum branching depth in the database.
	 *
	 * <p>
	 * A branch B1 that has {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} as origin has branching depth 1. A branch B2 that has B1 as origin has branching depth 2, etc. The master branch has branching depth zero by definition.
	 *
	 * @return The maximum branching depth. Always greater than or equal to zero.
	 *
	 * @see #getAverageBranchingDepth()
	 */
	public int getMaximumBranchingDepth();

	/**
	 * Returns the average branching depth.
	 *
	 * For a definition of "branching depth", please see {@link #getMaximumBranchingDepth()}.
	 *
	 * @return The average branching depth over all non-master branches. Will be zero if there are no branches other than the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master}, or a value greater than or equal to one if there is at least one branch.
	 */
	public double getAverageBranchingDepth();

	// =================================================================================================================
	// CHUNK DATA
	// =================================================================================================================

	/**
	 * Returns the number of chunks in the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
	 *
	 * <p>
	 * For all backends that do not support chunks, this method will return 1.
	 *
	 * @return the number of chunks in the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch. Guaranteed to be greater than or equal to 1.
	 */
	public int getMasterBranchChunks();

	/**
	 * Returns the number of chunks in the given branch.
	 *
	 * <p>
	 * For all backends that do not support chunks, this method will return 1 for all existing branches.
	 *
	 * @param branchName
	 *            the name of the branch to fetch the chunk count for. Must not be <code>null</code>.
	 * @return A value greater than or equal to 1 representing the number of chunks, if the given branch exists; otherwise zero.
	 */
	public int getNumberOfChunksInBranch(String branchName);

	/**
	 * Returns the average number of chunks in all branches ({@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch).
	 *
	 * @return The average number of chunks. Will be zero for backends that do not support chunks, or if there are no other branches. Will be a value greater than or equal to 1 in all other cases.
	 */
	public double getAverageNumberOfChunksPerNonMasterBranch();

	// =================================================================================================================
	// ACCESS PATTERNS
	// =================================================================================================================

	/**
	 * Returns the number of {@link ChronoDBTransaction#get(String)} operations executed on this database so far.
	 *
	 * @return The number of <code>get</code> operations. Never negative.
	 */
	public long getTotalNumberOfGetOperations();

	/**
	 * Returns the number of {@link ChronoDBTransaction#put(String, Object)} operations executed on this database so far.
	 *
	 * @return The number of <code>put</code> operations. Never negative.
	 */
	public long getTotalNumberOfPutOperations();

	/**
	 * Returns the number of {@link ChronoDBTransaction#remove(String)} operations executed on this database so far.
	 *
	 * @return The number of <code>remove</code> operations. Never negative.
	 */
	public long getTotalNumberOfRemoveOperations();

	/**
	 * Returns the number of {@link ChronoDBTransaction#get(String)} operations executed on this database within the given time range since the head revision.
	 *
	 * <p>
	 * Examples include:
	 *
	 * <pre>
	 * statistics.getNumberOfOperationsWithin(HeadMinus.TEN_SECONDS); // #1
	 * statistics.getNumberOfOperationsWithin(HeadMinus.ONE_HOUR); // #2
	 * </pre>
	 *
	 * <p>
	 * It is important to note that a <code>get</code> operation that belongs to the {@link HeadMinus#ONE_MINUTE} group will not be counted for the {@link HeadMinus#FIVE_MINUTES} group. Each <code>get</code> belongs to at most one time group.
	 *
	 * @param headMinus
	 *            The time distance between the <code>get</code> call timestamp and the head revision timestamp.
	 *
	 * @return The number of <code>get</code> operations within the given time delta since the head revision. Never negative.
	 *
	 * @see #getNumberOfGetOperationsInOlderHistory()
	 */
	public long getNumberOfGetOperationsWithin(HeadMinus headMinus);

	/**
	 * Returns the number of {@link ChronoDBTransaction#get(String)} operations executed on timestamps not retrievable via {@link #getNumberOfGetOperationsWithin(HeadMinus)}.
	 *
	 * @return The number of <code>get</code> operations executed on older history. Never negative.
	 */
	public long getNumberOfGetOperationsInOlderHistory();

	/**
	 * Returns the number of {@link ChronoDBTransaction#get(String)} operations executed on the given branch.
	 *
	 * @param branchName
	 *            The branch to retrieve the number of executed <code>get</code> operations for. Must not be <code>null</code>.
	 *
	 * @return The number of <code>get</code> operations on the given branch. May be zero. Will be zero for any non-existing branch.
	 */
	public long getNumberOfGetOperationsOnBranch(String branchName);

	/**
	 * Returns the number of {@link ChronoDBTransaction#put(String, Object)} operations executed on the given branch.
	 *
	 * @param branchName
	 *            The branch to retrieve the number of executed <code>put</code> operations for. Must not be <code>null</code>.
	 *
	 * @return The number of <code>put</code> operations on the given branch. May be zero. Will be zero for any non-existing branch.
	 */
	public long getNumberOfPutOperationsOnBranch(String branchName);

	/**
	 * Returns the number of {@link ChronoDBTransaction#remove(String)} operations executed on the given branch.
	 *
	 * @param branchName
	 *            The branch to retrieve the number of executed <code>remove</code> operations for. Must not be <code>null</code>.
	 *
	 * @return The number of <code>remove</code> operations on the given branch. May be zero. Will be zero for any non-existing branch.
	 */
	public long getNumberOfRemoveOperationsOnBranch(String branchName);

	// =================================================================================================================
	// CACHING
	// =================================================================================================================

	/**
	 * Returns the total number of entry cache misses, i.e. {@link ChronoDBTransaction#get(String)} calls that were not answered by the cache.
	 *
	 * @return The number of entry cache misses. Will be fixed at -1 if entry caching is disabled.
	 *
	 * @see #getNumberOfEntryCacheHits()
	 * @see #getNumberOfQueryCacheHits()
	 * @see #getNumberOfQueryCacheMisses()
	 */
	public long getNumberOfEntryCacheHits();

	/**
	 * Returns the total number entry cache hits, i.e. {@link ChronoDBTransaction#get(String)} calls that were answered by the cache.
	 *
	 * @return The number of entry cache hits. Will be fixed at -1 if entry caching is disabled.
	 *
	 * @see #getNumberOfEntryCacheMisses()
	 * @see #getNumberOfQueryCacheHits()
	 * @see #getNumberOfQueryCacheMisses()
	 */
	public long getNumberOfEntryCacheMisses();

	/**
	 * Returns the total number of query cache hits.
	 *
	 * @return The number of query cache hits. Will be fixed at -1 if query caching is disabled.
	 */
	public long getNumberOfQueryCacheHits();

	/**
	 * Returns the total number of query cache misses.
	 *
	 * @return The number of query cache misses. Will be fixed at -1 if query caching is disabled.
	 */
	public long getNumberOfQueryCacheMisses();

	// =================================================================================================================
	// INDEXING
	// =================================================================================================================

	/**
	 * Returns the immutable set of active secondary index names.
	 *
	 * @return The immutable set of secondary index names. Never <code>null</code>, may be empty.
	 */
	public Set<String> getActiveSecondaryIndices();

	/**
	 * Returns the total number of index documents that constitute the secondary index.
	 *
	 * @return The total number of index documents. Always greater than or equal to zero.
	 */
	public long getNumberOfIndexDocuments();

	// =================================================================================================================
	// RESOURCE USAGE
	// =================================================================================================================

	/**
	 * Estimates the footprint of this database instance on disk (in bytes).
	 *
	 * @return The estimated footprint of this database on disk, in bytes.
	 */
	public long getDiskFootprintInBytes();

	// =================================================================================================================
	// INNER CLASSES
	// =================================================================================================================

	public static enum HeadMinus {

		ONE_SECOND, FIVE_SECONDS, TEN_SECONDS, THIRTY_SECONDS, ONE_MINUTE, FIVE_MINUTES, TEN_MINUTES, THIRTY_MINUTES, ONE_HOUR, TWO_HOURS, THREE_HOURS

	}
}
