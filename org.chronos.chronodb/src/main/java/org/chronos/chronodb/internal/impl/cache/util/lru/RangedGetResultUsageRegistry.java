package org.chronos.chronodb.internal.impl.cache.util.lru;

import org.chronos.chronodb.internal.api.GetResult;

public class RangedGetResultUsageRegistry extends DefaultUsageRegistry<GetResult<?>> {

	public RangedGetResultUsageRegistry() {
		super(RangedGetResultUsageRegistry::extractTopic);
	}

	private static Object extractTopic(final GetResult<?> element) {
		return element.getRequestedKey();
	}

}
