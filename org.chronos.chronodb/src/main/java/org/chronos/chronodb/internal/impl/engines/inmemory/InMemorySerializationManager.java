package org.chronos.chronodb.internal.impl.engines.inmemory;

import org.chronos.chronodb.api.SerializationManager;
import org.chronos.common.serialization.KryoManager;

public class InMemorySerializationManager implements SerializationManager {

	public InMemorySerializationManager() {
	}

	@Override
	public byte[] serialize(final Object object) {
		return KryoManager.serialize(object);
	}

	@Override
	public Object deserialize(final byte[] serialForm) {
		return KryoManager.deserialize(serialForm);
	}

}
