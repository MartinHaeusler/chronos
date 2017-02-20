package org.chronos.chronograph.internal.impl.dumpformat.property;

import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.serialization.KryoManager;

public class BinaryPropertyDump extends AbstractPropertyDump {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private byte[] value;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected BinaryPropertyDump() {
		// serialization constructor
	}

	public BinaryPropertyDump(final PropertyRecord record) {
		super(record.getKey());
		this.value = KryoManager.serialize(record.getValue());
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Object getValue() {
		if (this.value == null) {
			return null;
		}
		return KryoManager.deserialize(this.value);
	}

}
