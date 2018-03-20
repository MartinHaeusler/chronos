package org.chronos.chronodb.internal.impl.conflict.strategies;

import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;

public class OverwriteWithTargetStrategy implements ConflictResolutionStrategy {

	public static OverwriteWithTargetStrategy INSTANCE = new OverwriteWithTargetStrategy();

	protected OverwriteWithTargetStrategy() {
	}

	@Override
	public Object resolve(final AtomicConflict conflict) {
		return conflict.getTargetValue();
	}

}
