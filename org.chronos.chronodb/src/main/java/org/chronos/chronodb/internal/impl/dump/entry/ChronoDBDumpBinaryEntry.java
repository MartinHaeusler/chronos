package org.chronos.chronodb.internal.impl.dump.entry;

import java.util.Base64;

import org.chronos.chronodb.api.key.ChronoIdentifier;

public class ChronoDBDumpBinaryEntry extends ChronoDBDumpEntry<byte[]> {

	private String binaryValue;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected ChronoDBDumpBinaryEntry() {
		// constructor for serialization purposes only
	}

	public ChronoDBDumpBinaryEntry(final ChronoIdentifier identifier, final byte[] value) {
		super(identifier);
		this.setValue(value);
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	@Override
	public void setValue(final byte[] value) {
		if (value == null) {
			this.binaryValue = null;
		} else {
			this.binaryValue = Base64.getEncoder().encodeToString(value);
		}
	}

	@Override
	public byte[] getValue() {
		if (this.binaryValue == null) {
			return null;
		} else {
			return Base64.getDecoder().decode(this.binaryValue);
		}
	}

}