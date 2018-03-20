package org.chronos.chronograph.api.transaction.conflict;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

public interface PropertyConflict {

	public String getPropertyKey();

	public Element getTransactionElement();

	public Property<?> getTransactionProperty();

	public Element getStoreElement();

	public Property<?> getStoreProperty();

	public Element getAncestorElement();

	public Property<?> getAncestorProperty();
}
