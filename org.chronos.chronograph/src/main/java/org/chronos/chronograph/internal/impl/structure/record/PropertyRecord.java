package org.chronos.chronograph.internal.impl.structure.record;

import static com.google.common.base.Preconditions.*;

import org.chronos.common.annotation.PersistentClass;
import org.chronos.common.serialization.KryoManager;

@PersistentClass("kryo")
public class PropertyRecord implements ElementRecord {

	private String key;
	private byte[] value;

	protected PropertyRecord() {
		// default constructor for serialization
	}

	public PropertyRecord(final String key, final Object value) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		this.key = key;
		this.value = KryoManager.serialize(value);
	}

	public String getKey() {
		return this.key;
	}

	public Object getValue() {
		return KryoManager.deserialize(this.value);
	}

}
