package org.chronos.chronograph.internal.impl.dumpformat.property;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractPropertyDump {

	private String key;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected AbstractPropertyDump() {
		// serialization constructor
	}

	protected AbstractPropertyDump(final String key) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		this.key = key;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public String getKey() {
		return this.key;
	}

	public abstract Object getValue();

}
