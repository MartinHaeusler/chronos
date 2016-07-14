package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.ChronoIndexModifications;
import org.chronos.chronodb.internal.api.query.SearchSpecification;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public abstract class AbstractIndexManagerBackend implements IndexManagerBackend {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	protected final ChronoDB owningDB;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected AbstractIndexManagerBackend(final ChronoDB owningDB) {
		checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
		this.owningDB = owningDB;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Collection<ChronoIndexDocument> getMatchingDocuments(final long timestamp, final Branch branch,
			final SearchSpecification searchSpec) {
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
		// fetch the list of branches we need to process by fetching the recursive origin branches (starting at master)
		List<Branch> branches = branch.getOriginsRecursive();
		// always add the branch we need to actually process to the end of the list
		branches.add(branch);
		// prepare a mapping from qualified keys to document that holds our result
		SetMultimap<QualifiedKey, ChronoIndexDocument> resultMap = HashMultimap.create();
		// now, iterate over the list and repeat the matching algorithm for every branch:
		// 1) Collect the matches from the origin branch
		// 2) Remove from these matches all of those which were deleted in our current branch
		// 3) Add the matches local to our current branch
		for (Branch currentBranch : branches) {
			String branchName = currentBranch.getName();
			// check if we have a non-empty result set (in that case, we have to respect branch-local deletions)
			if (resultMap.isEmpty() == false) {
				// find the branch-local deletions
				Collection<ChronoIndexDocument> localDeletions = this.getTerminatedBranchLocalDocuments(timestamp,
						branchName, searchSpec);
				// remove the local deletions from our overall result
				for (ChronoIndexDocument localDeletion : localDeletions) {
					QualifiedKey qKey = QualifiedKey.create(localDeletion.getKeyspace(), localDeletion.getKey());
					Set<ChronoIndexDocument> docsToCheck = Sets.newHashSet(resultMap.get(qKey));
					// remove all those documents which name the same value as the local deletion
					for (ChronoIndexDocument document : docsToCheck) {
						if (document.getIndexedValue().equals(localDeletion.getIndexedValue())) {
							// this document was deleted in the current branch, remove it from the overall result
							resultMap.remove(qKey, document);
						}
					}
				}
			}
			// find the branch-local matches
			Collection<ChronoIndexDocument> localMatches = this.getMatchingBranchLocalDocuments(timestamp, branchName,
					searchSpec);
			// add the local matches to our overall result
			for (ChronoIndexDocument localMatch : localMatches) {
				QualifiedKey qKey = QualifiedKey.create(localMatch.getKeyspace(), localMatch.getKey());
				resultMap.put(qKey, localMatch);
			}
		}
		// finally, we are only interested in the values from the result map, as they form our query result.
		return Collections.unmodifiableSet(Sets.newHashSet(resultMap.values()));
	}

	@Override
	public void rollback(final Set<String> branches, final long timestamp) {
		checkNotNull(branches, "Precondition violation - argument 'branches' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		// "null" in the second argument means "roll back all keys"
		this.rollbackInternal(timestamp, branches, null);
	}

	@Override
	public void rollback(final Set<String> branches, final long timestamp, final Set<QualifiedKey> keys) {
		checkNotNull(branches, "Precondition violation - argument 'branches' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		checkNotNull(keys, "Precondition violation - argument 'keys' must not be NULL!");
		this.rollbackInternal(timestamp, branches, keys);
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	/**
	 * Performs the actual rollback operation on this index.
	 *
	 * @param timestamp
	 *            The timestamp to roll back to. Must not be negative.
	 * @param keys
	 *            The set of keys to roll back. If the set is <code>null</code>, <b>all</b> keys will be rolled back. If
	 *            the set is empty, no keys will be rolled back. If the set is non-empty, only the given keys will be
	 *            rolled back.
	 */
	protected void rollbackInternal(final long timestamp, final Set<String> branches, final Set<QualifiedKey> keys) {
		if (keys != null && keys.isEmpty()) {
			// we have no keys to roll back; method is a no-op.
			return;
		}
		// get the set of all documents that were added and/or modified at or after the given timestamp
		Set<ChronoIndexDocument> documents = this.getDocumentsTouchedAtOrAfterTimestamp(timestamp);
		// prepare the index modification set
		ChronoIndexModifications indexModifications = ChronoIndexModifications.create();
		for (ChronoIndexDocument document : documents) {
			// check if we need to process this document (i.e. if it is relevant for the rollback)
			if (branches.contains(document.getBranch()) == false) {
				// document does not belong to a relevant branch; skip it
				continue;
			}
			if (this.isDocumentRelevantForRollback(document, keys) == false) {
				continue;
			}
			// document is relevant, perform the rollback
			long validFrom = document.getValidFromTimestamp();
			long validTo = document.getValidToTimestamp();
			// check if the document was created strictly after our timestamp
			if (validFrom > timestamp) {
				// delete this document
				indexModifications.addDocumentDeletion(document);
			} else {
				// check if the document validity has been trimmed to a value after our timestamp
				if (validTo < Long.MAX_VALUE && validTo >= timestamp) {
					// reset the document validity to infinity
					indexModifications.addDocumentValidityTermination(document, Long.MAX_VALUE);
				}
			}
		}
		if (indexModifications.isEmpty() == false) {
			// perform the deletions
			this.applyModifications(indexModifications);
		}
	}

	/**
	 * A helper method to determine if the given document is relevant for rollback with respect to the set of keys to
	 * roll back.
	 *
	 * @param document
	 *            The document to check. Must not be <code>null</code>.
	 * @param keys
	 *            The set of keys to roll back. May be <code>null</code> to indicate that all keys will be rolled back.
	 * @return If the given set is <code>null</code>, this method will always return <code>true</code>. If the set is
	 *         empty, this method will always return <code>false</code>. Otherwise, this method returns
	 *         <code>true</code> if and only if the document is bound to a qualified key that is in this set.
	 */
	protected boolean isDocumentRelevantForRollback(final ChronoIndexDocument document, final Set<QualifiedKey> keys) {
		if (keys == null) {
			// use all documents
			return true;
		}
		if (keys.isEmpty()) {
			// use no documents
			return false;
		}
		// check if the document is bound to one of the keys we should roll back
		String keyspace = document.getKeyspace();
		String key = document.getKey();
		QualifiedKey qKey = QualifiedKey.create(keyspace, key);
		if (keys.contains(qKey) == false) {
			return false;
		} else {
			return true;
		}
	}

	// =====================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =====================================================================================================================

	/**
	 * Returns the set of documents that were added and/or modified exactly at the given timestamp, or after the given
	 * timestamp.
	 *
	 * @param timestamp
	 *            The timestamp in question. Must not be negative.
	 *
	 * @return The set of index documents that were added and/or modified exactly at or after the given timestamp. May
	 *         be empty, but never <code>null</code>.
	 */
	protected abstract Set<ChronoIndexDocument> getDocumentsTouchedAtOrAfterTimestamp(long timestamp);

	protected abstract Collection<ChronoIndexDocument> getTerminatedBranchLocalDocuments(long timestamp,
			String branchName, SearchSpecification searchSpec);

	protected abstract Collection<ChronoIndexDocument> getMatchingBranchLocalDocuments(long timestamp,
			String branchName, SearchSpecification searchSpec);
}
