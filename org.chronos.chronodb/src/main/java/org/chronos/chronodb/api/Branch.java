package org.chronos.chronodb.api;

import java.util.List;

import org.chronos.chronodb.internal.impl.IBranchMetadata;

/**
 * A {@link Branch} represents a single stream of changes in the versioning system.
 *
 * <p>
 * Branches work much like in document versioning systems, such as GIT or SVN. Every branch has an "{@linkplain #getOrigin() origin}" (or "parent") branch from which it was created, as well as a "{@linkplain #getBranchingTimestamp() branching timestamp}" that reflects the point in time from which this branch was created. There is one special branch, which is the {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master branch}. It is the transitive origin of all other branches. It always has the same name, an origin of <code>null</code>, and a branching timestamp of zero. Unlike other branches, the master branch is created by default and always exists.
 *
 * <p>
 * All branches are uniquely identified by their name.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface Branch {

	/**
	 * Returns the name of this branch, which also acts as its unique identifier.
	 *
	 * @return The branch name. Never <code>null</code>. Branch names are unique.
	 */
	public String getName();

	/**
	 * Returns the branch from which this branch originates.
	 *
	 * @return The origin (parent) branch. May be <code>null</code> if this branch is the master branch.
	 */
	public Branch getOrigin();

	/**
	 * Returns the timestamp at which this branch was created from the origin (parent) branch.
	 *
	 * @return The branching timestamp. Never negative.
	 */
	public long getBranchingTimestamp();

	/**
	 * Returns the list of direct and transitive origin branches.
	 *
	 * <p>
	 * The first entry in the list will always be the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch. The remaining entries are the descendants of the previous branch in the list which are also direct or transitive origins of the current branch.
	 *
	 * <p>
	 * For example, if there were the following branching actions:
	 * <ol>
	 * <li>master is forked into new branch A
	 * <li>A is forked into new branch B
	 * <li>B is forked into new branch C
	 * </ol>
	 *
	 * ... and <code>C.getOriginsRecursive()</code> is invoked, then the returned list will consist of <code>[master, A, B]</code> (in exactly this order).
	 *
	 * @return The list of origin branches. Will be empty for the master branch, and will start with the master branch for all other branches. Never <code>null</code>. The returned list is a calculated object which may freely be modified without changing any internal state.
	 */
	public List<Branch> getOriginsRecursive();

	/**
	 * Returns the "now" timestamp on this branch, i.e. the timestamp at which the last full commit was successfully executed.
	 *
	 * @return The "now" timestamp. The minimum is the branching timestamp (or zero for the master branch). Never negative.
	 */
	public long getNow();

	/**
	 * Returns the name of the directory where this branch is stored.
	 *
	 * <p>
	 * For backends that are not file based, this value may be <code>null</code>.
	 *
	 * @return The name of the directory where this branch is located. May be <code>null</code> if this ChronoDB instance is using a backend that is not file-based.
	 */
	public String getDirectoryName();

	/**
	 * Returns the immutable metadata of this branch.
	 *
	 * @return The metadata object. Never <code>null</code>
	 *
	 * @since 0.6.0
	 */
	public IBranchMetadata getMetadata();
}
