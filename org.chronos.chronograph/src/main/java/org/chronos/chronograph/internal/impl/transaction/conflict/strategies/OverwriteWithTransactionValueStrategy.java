package org.chronos.chronograph.internal.impl.transaction.conflict.strategies;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflict;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflictResolutionStrategy;

public class OverwriteWithTransactionValueStrategy implements PropertyConflictResolutionStrategy {

	public static final OverwriteWithTransactionValueStrategy INSTANCE = new OverwriteWithTransactionValueStrategy();

	private OverwriteWithTransactionValueStrategy() {
		// singleton
	}

	@Override
	public Object resolve(final PropertyConflict conflict) {
		Property<?> property = conflict.getTransactionProperty();
		if (property.isPresent()) {
			return property.value();
		} else {
			return null;
		}
	}

}
