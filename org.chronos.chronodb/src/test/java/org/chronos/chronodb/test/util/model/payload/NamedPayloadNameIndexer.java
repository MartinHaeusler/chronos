package org.chronos.chronodb.test.util.model.payload;

import java.util.Collections;
import java.util.Set;

import org.chronos.chronodb.api.ChronoIndexer;

public class NamedPayloadNameIndexer implements ChronoIndexer {

	private final boolean toLowerCase;

	public NamedPayloadNameIndexer() {
		this(false);
	}

	public NamedPayloadNameIndexer(final boolean toLowerCase) {
		this.toLowerCase = toLowerCase;
	}

	@Override
	public boolean canIndex(final Object object) {
		return object != null && object instanceof NamedPayload;
	}

	@Override
	public Set<String> getIndexValues(final Object object) {
		NamedPayload payload = (NamedPayload) object;
		String name = payload.getName();
		if (this.toLowerCase) {
			return Collections.singleton(name.toLowerCase());
		} else {
			return Collections.singleton(name);
		}
	}

}