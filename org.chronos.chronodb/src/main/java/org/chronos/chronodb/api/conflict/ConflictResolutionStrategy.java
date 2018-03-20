package org.chronos.chronodb.api.conflict;

import org.chronos.chronodb.internal.impl.conflict.strategies.DoNotMergeStrategy;
import org.chronos.chronodb.internal.impl.conflict.strategies.OverwriteWithSourceStrategy;
import org.chronos.chronodb.internal.impl.conflict.strategies.OverwriteWithTargetStrategy;

public interface ConflictResolutionStrategy {

	// =================================================================================================================
	// DEFAULT STRATEGIES
	// =================================================================================================================

	public static ConflictResolutionStrategy OVERWRITE_WITH_SOURCE = OverwriteWithSourceStrategy.INSTANCE;
	public static ConflictResolutionStrategy OVERWRITE_WITH_TARGET = OverwriteWithTargetStrategy.INSTANCE;
	public static ConflictResolutionStrategy DO_NOT_MERGE = DoNotMergeStrategy.INSTANCE;

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	public Object resolve(AtomicConflict conflict);

}
