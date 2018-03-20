package org.chronos.chronograph.internal.impl.transaction.conflict.strategies;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflict;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflictResolutionStrategy;

public class OverwriteWithStoreValueStrategy implements PropertyConflictResolutionStrategy {

	public static OverwriteWithStoreValueStrategy INSTANCE = new OverwriteWithStoreValueStrategy();

	private OverwriteWithStoreValueStrategy() {
		// singleton
	}

	@Override
	public Object resolve(final PropertyConflict conflict) {
		Property<?> property = conflict.getStoreProperty();
		if (property.isPresent()) {
			return property.value();
		} else {
			return null;
		}
	}

}
