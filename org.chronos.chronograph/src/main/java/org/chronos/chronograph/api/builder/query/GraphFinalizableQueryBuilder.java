package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.structure.Element;

public interface GraphFinalizableQueryBuilder<E extends Element> extends GraphQueryBaseBuilder<E, GraphFinalizableQueryBuilder<E>>, GraphQueryBuilderFinalizer<E> {

	public long count();

	public GraphQueryBuilder<E> and();

	public GraphQueryBuilder<E> or();

}
