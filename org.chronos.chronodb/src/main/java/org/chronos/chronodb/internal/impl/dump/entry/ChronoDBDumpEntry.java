package org.chronos.chronodb.internal.impl.dump.entry;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.dump.base.ChronoDBDumpElement;

public abstract class ChronoDBDumpEntry<T> extends ChronoDBDumpElement {

	private long timestamp;
	private String branch;
	private String keyspace;
	private String key;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronoDBDumpEntry() {
		// serialization constructor
		this.timestamp = -1L;
	}

	public ChronoDBDumpEntry(final ChronoIdentifier identifier) {
		this.setChronoIdentifier(identifier);
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	public void setChronoIdentifier(final ChronoIdentifier identifier) {
		if (identifier == null) {
			// reset the identifier data
			this.timestamp = -1L;
			this.branch = null;
			this.keyspace = null;
			this.key = null;
		} else {
			// extract the data from the identifier
			this.timestamp = identifier.getTimestamp();
			this.branch = identifier.getBranchName();
			this.keyspace = identifier.getKeyspace();
			this.key = identifier.getKey();
		}
	}

	public ChronoIdentifier getChronoIdentifier() {
		if (this.timestamp < 0 || this.branch == null || this.keyspace == null || this.key == null) {
			// identifier was not set yet
			return null;
		}
		return ChronoIdentifier.create(this.branch, this.timestamp, this.keyspace, this.key);
	}

	public abstract T getValue();

	public abstract void setValue(T value);
}
