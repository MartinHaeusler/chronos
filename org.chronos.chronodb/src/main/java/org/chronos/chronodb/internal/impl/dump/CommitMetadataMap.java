package org.chronos.chronodb.internal.impl.dump;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.chronos.chronodb.api.key.ChronoIdentifier;

import com.google.common.collect.Maps;

public class CommitMetadataMap {

	/** Branch name -> commit timestamp -> commit metadata */
	private final Map<String, SortedMap<Long, Object>> branchToCommitTimestamps;

	public CommitMetadataMap() {
		this.branchToCommitTimestamps = Maps.newHashMap();
	}

	/**
	 * Adds the given identifier to the commit timestamp map.
	 *
	 * <p>
	 * It will have <code>null</code> associated as commit metadata. If a commit metadata object already exists for that timestamp, the metadata will be kept and will <b>not</b> be overwritten with <code>null</code>.
	 *
	 * @param identifier
	 *            The {@link ChronoIdentifier} to add to the commit map. Must not be <code>null</code>.
	 */
	public void addEntry(final ChronoIdentifier identifier) {
		checkNotNull(identifier, "Precondition violation - argument 'identifier' must not be NULL!");
		this.addEntry(identifier.getBranchName(), identifier.getTimestamp(), null);
	}

	/**
	 * Adds the given commit timestamp (and metadata) to this map.
	 *
	 *
	 * @param branch
	 *            The name of the branch on which the commit occurred. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp at which the commit occurred. Must not be negative.
	 * @param metadata
	 *            The metadata associated with the branch. May be <code>null</code>.
	 */
	public void addEntry(final String branch, final long timestamp, final Object metadata) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'branch' must not be negative!");
		SortedMap<Long, Object> map = this.branchToCommitTimestamps.get(branch);
		if (map == null) {
			map = Maps.newTreeMap();
			this.branchToCommitTimestamps.put(branch, map);
		}
		map.putIfAbsent(timestamp, metadata);
	}

	/**
	 * Returns the set of branch names for which commits are available.
	 *
	 * @return Returns an unmodifiable view on the set of branches which have commits assigned to them in this map. Never <code>null</code>.
	 */
	public Set<String> getContainedBranches() {
		return Collections.unmodifiableSet(this.branchToCommitTimestamps.keySet());
	}

	/**
	 * Returns the commit timestamps (and associated metadata) for the given branch.
	 *
	 * <p>
	 * The keys of the map are the commit timestamps, the values are the metadata associated with the commit (which may be <code>null</code>).
	 *
	 * @param branchName
	 *            The name of the branch to get the commits for. Must not be <code>null</code>.
	 * @return An unmodifiable view on the map from timestamp to commit metadata for the given branch. May be empty, but never <code>null</code>.
	 */
	public SortedMap<Long, Object> getCommitMetadataForBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		SortedMap<Long, Object> map = this.branchToCommitTimestamps.get(branchName);
		if (map == null) {
			map = Collections.emptySortedMap();
		}
		return Collections.unmodifiableSortedMap(map);
	}

}
