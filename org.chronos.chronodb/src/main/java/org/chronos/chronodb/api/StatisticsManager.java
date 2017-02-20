package org.chronos.chronodb.api;

/**
 * The {@link StatisticsManager} is the entry point to all statistic-related capabilities in the public API.
 *
 * <p>
 * The primary purpose of this class is to produce statistic report objects.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface StatisticsManager {

	/**
	 * Calculates and returns the global statistics for this {@link ChronoDB} instance.
	 *
	 * <p>
	 * Please note that running this method can potentially take a while. The returned object is immutable. Every invocation of this method will produce a new, independent statistics object.
	 *
	 * @return The global statistics. Never <code>null</code>.
	 */
	public ChronoDBStatistics calculateGlobalStatistics();

	/**
	 * Returns the statistics for the "head portion" of the given branch.
	 *
	 * <p>
	 * The definition of what the "head portion" is, is up to the backend at hand. In general, it refers to the more recent history.
	 *
	 * @param branchName
	 *            The name of the branch to retrieve the statistics for. Must not be <code>null</code>, must refer to an existing branch.
	 *
	 * @return The statistics. Never <code>null</code>.
	 */
	public BranchHeadStatistics calculateBranchHeadStatistics(String branchName);

}
