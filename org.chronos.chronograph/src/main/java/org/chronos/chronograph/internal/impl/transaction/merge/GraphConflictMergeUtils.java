package org.chronos.chronograph.internal.impl.transaction.merge;

import static com.google.common.base.Preconditions.*;

import java.util.Objects;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflict;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflictResolutionStrategy;
import org.chronos.chronograph.internal.impl.transaction.conflict.PropertyConflictImpl;

import com.google.common.collect.Sets;

public class GraphConflictMergeUtils {

	public static void mergeProperties(final Element element, final Element storeElement, final Element ancestorElement,
			final PropertyConflictResolutionStrategy strategy) {
		checkNotNull(element, "Precondition violation - argument 'element' must not be NULL!");
		checkNotNull(storeElement, "Precondition violation - argument 'storeElement' must not be NULL!");
		checkNotNull(strategy, "Precondition violation - argument 'strategy' must not be NULL!");
		// get all element property keys
		Set<String> elementKeys = Sets.newHashSet(element.keys());
		Set<String> storeElementKeys = Sets.newHashSet(storeElement.keys());
		Set<String> allKeys = Sets.union(elementKeys, storeElementKeys);
		for (String propertyKey : allKeys) {
			mergeSingleProperty(propertyKey, element, storeElement, ancestorElement, strategy);
		}
	}

	private static void mergeSingleProperty(final String propertyKey, final Element element, final Element storeElement,
			final Element ancestorElement, final PropertyConflictResolutionStrategy strategy) {
		Property<?> elementProperty = element.property(propertyKey);
		Property<?> storeProperty = storeElement.property(propertyKey);
		// resolve the ancestor property, if any
		Property<?> ancestorProperty = Property.empty();
		if (ancestorElement != null) {
			ancestorProperty = ancestorElement.property(propertyKey);
		}
		if (elementProperty.isPresent() && !storeProperty.isPresent()) {
			// either the property was added by the transaction, or it was removed in the store.
			if (ancestorProperty.isPresent() && Objects.equals(ancestorProperty.value(), elementProperty.value())) {
				// property was deleted in the store and is unchanged in our transaction, remove it
				elementProperty.remove();
				return;
			} else if (ancestorProperty.isPresent() == false) {
				// property was newly added in the current transaction, keep it
				return;
			}
		}
		if (!elementProperty.isPresent() && storeProperty.isPresent()) {
			// the property must have either been added in the store, or deleted in our transaction.
			// check the common ancestor.
			if (ancestorProperty.isPresent() && Objects.equals(ancestorProperty.value(), storeProperty.value())) {
				// the property has been removed in this transaction, keep it that way.
				return;
			} else if (ancestorProperty.isPresent() == false) {
				// the property has been added in the store, add it to the transaction
				element.property(propertyKey, storeProperty.value());
				return;
			}
		}
		if (elementProperty.isPresent() && storeProperty.isPresent()) {
			// property is present in transaction and store, compare the values
			Object value = elementProperty.value();
			Object storeValue = storeProperty.value();
			if (Objects.equals(value, storeValue)) {
				return;
			} else {
				// values are conflicting, check if any side is unchanged w.r.t. ancestor
				if (ancestorProperty.isPresent()) {
					// there is a common ancestor, check if either side is unchanged
					Object ancestorValue = ancestorProperty.value();
					if (Objects.equals(value, ancestorValue)) {
						// transaction property is unchanged, we missed an update in the store.
						// use the value from the stored property.
						element.property(propertyKey, storeValue);
						return;
					} else if (Objects.equals(storeValue, ancestorValue)) {
						// transaction property has been modified, store is unmodified, keep the change
						return;
					}
				}
			}
		}
		// in any other case, we have a conflict (possibly without common ancestor)
		PropertyConflict conflict = new PropertyConflictImpl(propertyKey, element, elementProperty, storeElement,
				storeProperty, ancestorElement, ancestorProperty);
		resolveConflict(conflict, strategy);
		return;
	}

	private static void resolveConflict(final PropertyConflict conflict,
			final PropertyConflictResolutionStrategy strategy) {
		Object newValue = strategy.resolve(conflict);
		if (newValue == null) {
			conflict.getTransactionProperty().remove();
		} else {
			String key = conflict.getPropertyKey();
			conflict.getTransactionElement().property(key, newValue);
		}
	}

}
