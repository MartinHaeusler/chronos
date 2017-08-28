package org.chronos.chronodb.internal.impl.dump.meta;

import static com.google.common.base.Preconditions.*;

public class CommitDumpMetadata {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private long timestamp;
	private String branch;
	private Object metadata;

	// =================================================================================================================
	// CONSTRUCTORS
	// =================================================================================================================

	public CommitDumpMetadata() {
		// default constructor for (de-)serialization
	}

	public CommitDumpMetadata(final String branch, final long timestamp, final Object metadata) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
		this.branch = branch;
		this.timestamp = timestamp;
		this.metadata = metadata;
	}

	// =================================================================================================================
	// GETTERS & SETTERS
	// =================================================================================================================

	public long getTimestamp() {
		return this.timestamp;
	}

	public void setTimestamp(final long timestamp) {
		this.timestamp = timestamp;
	}

	public String getBranch() {
		return this.branch;
	}

	public void setBranch(final String branch) {
		this.branch = branch;
	}

	public Object getMetadata() {
		return this.metadata;
	}

	public void setMetadata(final Object metadata) {
		this.metadata = metadata;
	}

}
