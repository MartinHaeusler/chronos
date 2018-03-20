package org.chronos.chronograph.internal.impl.transaction.conflict;

import static com.google.common.base.Preconditions.*;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.chronos.chronograph.api.transaction.conflict.PropertyConflict;

public class PropertyConflictImpl implements PropertyConflict {

	private final String propertyKey;
	private final Element transactionElement;
	private final Property<?> transactionProperty;
	private final Element storeElement;
	private final Property<?> storeProperty;
	private final Element ancestorElement;
	private final Property<?> ancestorProperty;

	public PropertyConflictImpl(final String propertyKey, final Element transactionElement,
			final Property<?> transactionProperty, final Element storeElement, final Property<?> storeProperty,
			final Element ancestorElement, final Property<?> ancestorProperty) {
		checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
		checkNotNull(ancestorElement, "Precondition violation - argument 'ancestorElement' must not be NULL!");
		checkNotNull(transactionProperty, "Precondition violation - argument 'transactionProperty' must not be NULL!");
		checkNotNull(storeElement, "Precondition violation - argument 'storeElement' must not be NULL!");
		checkNotNull(storeProperty, "Precondition violation - argument 'storeProperty' must not be NULL!");
		this.propertyKey = propertyKey;
		this.transactionElement = transactionElement;
		this.transactionProperty = transactionProperty;
		this.storeElement = storeElement;
		this.storeProperty = storeProperty;
		this.ancestorElement = ancestorElement;
		this.ancestorProperty = ancestorProperty;
	}

	@Override
	public String getPropertyKey() {
		return this.propertyKey;
	}

	@Override
	public Element getTransactionElement() {
		return this.transactionElement;
	}

	@Override
	public Property<?> getTransactionProperty() {
		return this.transactionProperty;
	}

	@Override
	public Element getStoreElement() {
		return this.storeElement;
	}

	@Override
	public Property<?> getStoreProperty() {
		return this.storeProperty;
	}

	@Override
	public Element getAncestorElement() {
		return this.ancestorElement;
	}

	@Override
	public Property<?> getAncestorProperty() {
		return this.ancestorProperty;
	}

}
