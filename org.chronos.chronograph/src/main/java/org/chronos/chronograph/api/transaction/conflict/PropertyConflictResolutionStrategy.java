package org.chronos.chronograph.api.transaction.conflict;

import org.chronos.chronograph.internal.impl.transaction.conflict.strategies.OverwriteWithStoreValueStrategy;
import org.chronos.chronograph.internal.impl.transaction.conflict.strategies.OverwriteWithTransactionValueStrategy;
import org.chronos.chronograph.internal.impl.transaction.conflict.strategies.ThrowExceptionStrategy;

public interface PropertyConflictResolutionStrategy {

	// =================================================================================================================
	// CONSTANTS
	// =================================================================================================================

	public static final PropertyConflictResolutionStrategy OVERWRITE_WITH_TRANSACTION_VALUE = OverwriteWithTransactionValueStrategy.INSTANCE;
	public static final PropertyConflictResolutionStrategy OVERWRITE_WITH_STORE_VALUE = OverwriteWithStoreValueStrategy.INSTANCE;
	public static final PropertyConflictResolutionStrategy DO_NOT_MERGE = ThrowExceptionStrategy.INSTANCE;

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	public Object resolve(PropertyConflict conflict);

}
