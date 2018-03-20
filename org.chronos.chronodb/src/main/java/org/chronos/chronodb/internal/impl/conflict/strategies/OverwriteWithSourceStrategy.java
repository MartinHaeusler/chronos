package org.chronos.chronodb.internal.impl.conflict.strategies;

import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;

public class OverwriteWithSourceStrategy implements ConflictResolutionStrategy {

	public static final OverwriteWithSourceStrategy INSTANCE = new OverwriteWithSourceStrategy();

	protected OverwriteWithSourceStrategy() {
	}

	@Override
	public Object resolve(final AtomicConflict conflict) {
		return conflict.getSourceValue();
	}

}
