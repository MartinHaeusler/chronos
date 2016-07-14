package org.chronos.chronodb.internal.impl.cache.util.lru;

import org.chronos.chronodb.internal.api.RangedGetResult;

public class RangedGetResultUsageRegistry extends DefaultUsageRegistry<RangedGetResult<?>> {

	public RangedGetResultUsageRegistry() {
		super(RangedGetResultUsageRegistry::extractTopic);
	}

	private static Object extractTopic(final Object element) {
		RangedGetResult<?> rangedGetResult = (RangedGetResult<?>) element;
		return rangedGetResult.getRequestedKey();
	}

}
