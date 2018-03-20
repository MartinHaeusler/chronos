package org.chronos.chronodb.internal.impl.conflict.strategies;

import org.chronos.chronodb.api.conflict.AtomicConflict;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitConflictException;

public class DoNotMergeStrategy implements ConflictResolutionStrategy {

	public static final DoNotMergeStrategy INSTANCE = new DoNotMergeStrategy();

	protected DoNotMergeStrategy() {

	}

	@Override
	public Object resolve(final AtomicConflict conflict) {
		throw new ChronoDBCommitConflictException("There are conflicting commits on "
				+ conflict.getSourceKey().toQualifiedKey() + " (and potentially others)!");
	}

}
