package org.chronos.chronodb.internal.impl.dump.meta;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.chronos.chronodb.internal.impl.dump.base.ChronoDBDumpElement;
import org.chronos.common.version.ChronosVersion;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ChronoDBDumpMetadata extends ChronoDBDumpElement {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	/** The date (and time) at which this element was created. */
	private Date creationDate;
	/** The version of Chronos that created this element. */
	private String chronosVersion;

	/**
	 * The metadata associated with each commit.
	 *
	 * @since 0.6.1
	 */
	private List<CommitDumpMetadata> commitMetadata = Lists.newArrayList();
	/** The metetadata associated to each branch. */
	private Set<BranchDumpMetadata> branchMetadata = Sets.newHashSet();
	/** The indexers and corresponding metadata. */
	private Set<IndexerDumpMetadata> indexerMetadata = Sets.newHashSet();

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronoDBDumpMetadata() {
		// default constructor, also needed for serialization
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	public void setChronosVersion(final ChronosVersion version) {
		if (version == null) {
			this.chronosVersion = null;
			return;
		}
		this.chronosVersion = version.toString();
	}

	public ChronosVersion getChronosVersion() {
		if (this.chronosVersion == null) {
			return null;
		}
		return ChronosVersion.parse(this.chronosVersion);
	}

	public void setCreationDate(final Date date) {
		if (date == null) {
			this.creationDate = null;
			return;
		}
		this.creationDate = new Date(date.getTime());
	}

	public Date getCreationDate() {
		if (this.creationDate == null) {
			return null;
		}
		return new Date(this.creationDate.getTime());
	}

	public Set<BranchDumpMetadata> getBranchDumpMetadata() {
		return this.branchMetadata;
	}

	public Set<IndexerDumpMetadata> getIndexerDumpMetadata() {
		return this.indexerMetadata;
	}

	/**
	 * Returns the commit metadata stored in this dump.
	 *
	 * <p>
	 * The returned list is the internal representation; modifications to this list will be visible in the owning object!
	 *
	 * @return The list of commit metadata entries.
	 *
	 * @since 0.6.1
	 */
	public List<CommitDumpMetadata> getCommitDumpMetadata() {
		if (this.commitMetadata == null) {
			// some dumps maybe do not contain this data, and
			// deserializers might assing NULL to the field. Just
			// to be safe, let's re-initialize the list in this case.
			this.commitMetadata = Lists.newArrayList();
		}
		return this.commitMetadata;
	}
}
