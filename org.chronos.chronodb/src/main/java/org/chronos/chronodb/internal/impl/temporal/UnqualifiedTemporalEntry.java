package org.chronos.chronodb.internal.impl.temporal;

public class UnqualifiedTemporalEntry {

	private final UnqualifiedTemporalKey key;
	private final byte[] value;

	public UnqualifiedTemporalEntry(final UnqualifiedTemporalKey key, final byte[] value) {
		this.key = key;
		this.value = value;
	}

	public UnqualifiedTemporalKey getKey() {
		return this.key;
	}

	public byte[] getValue() {
		return this.value;
	}

}
