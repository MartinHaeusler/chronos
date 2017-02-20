package org.chronos.chronograph.api.index;

import org.apache.tinkerpop.gremlin.structure.Element;

public interface ChronoGraphIndex {

	public String getIndexedProperty();

	public String getBackendIndexKey();

	public Class<? extends Element> getIndexedElementClass();

}
