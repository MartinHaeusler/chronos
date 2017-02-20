package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.structure.Element;

public interface GraphQueryBuilder<E extends Element> extends GraphQueryBaseBuilder<E, GraphQueryBuilder<E>> {

	public GraphWhereBuilder<E> where(String property);

}
