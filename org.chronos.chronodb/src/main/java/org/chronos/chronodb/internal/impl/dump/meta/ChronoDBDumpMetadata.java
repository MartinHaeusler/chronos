package org.chronos.chronodb.internal.impl.dump.meta;

import java.util.Date;
import java.util.Set;

import org.chronos.chronodb.internal.impl.dump.base.ChronoDBDumpElement;
import org.chronos.common.version.ChronosVersion;

import com.google.common.collect.Sets;

public class ChronoDBDumpMetadata extends ChronoDBDumpElement {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private Date creationDate;
	private String chronosVersion;
	private Set<BranchDumpMetadata> branchMetadata = Sets.newHashSet();
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
}
