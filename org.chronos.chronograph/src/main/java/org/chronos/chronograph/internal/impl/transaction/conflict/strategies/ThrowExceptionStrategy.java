package org.chronos.chronograph.internal.impl.transaction.conflict.strategies;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.exceptions.ChronoGraphCommitConflictException;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflict;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflictResolutionStrategy;

public class ThrowExceptionStrategy implements PropertyConflictResolutionStrategy {

	public static final ThrowExceptionStrategy INSTANCE = new ThrowExceptionStrategy();

	private ThrowExceptionStrategy() {
		// singleton
	}

	@Override
	public Object resolve(final PropertyConflict conflict) {
		String elementClass = conflict.getTransactionElement() instanceof Vertex ? "Vertex" : "Edge";
		throw new ChronoGraphCommitConflictException("Commit conflict on " + elementClass + " ("
				+ conflict.getTransactionElement().id() + ") at property '" + conflict.getPropertyKey() + "'!");
	}

}
