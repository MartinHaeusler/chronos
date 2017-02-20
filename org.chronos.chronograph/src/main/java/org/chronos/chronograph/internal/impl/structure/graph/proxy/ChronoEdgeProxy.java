package org.chronos.chronograph.internal.impl.structure.graph.proxy;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;

public class ChronoEdgeProxy extends AbstractElementProxy<ChronoEdgeImpl> implements ChronoEdge {

	public ChronoEdgeProxy(final ChronoGraph graph, final String id) {
		super(graph, id);
	}

	public ChronoEdgeProxy(final ChronoEdgeImpl edge) {
		super(edge.graph(), edge.id());
		this.element = edge;
	}

	// =================================================================================================================
	// TINKERPOP API
	// =================================================================================================================

	@Override
	public Iterator<Vertex> vertices(final Direction direction) {
		return this.getElement().vertices(direction);
	}

	@Override
	public <V> Property<V> property(final String key, final V value) {
		return this.getElement().property(key, value);
	}

	@Override
	public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
		return this.getElement().properties(propertyKeys);
	}

	@Override
	public String toString() {
		return StringFactory.edgeString(this);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	protected ChronoEdgeImpl loadElement(final ChronoGraphTransaction tx, final String id) {
		ChronoEdge edge = (ChronoEdge) tx.getEdge(id);
		return ChronoProxyUtil.resolveEdgeProxy(edge);
	}

	@Override
	protected void registerProxyAtTransaction(final ChronoGraphTransaction transaction) {
		transaction.getContext().registerEdgeProxyInCache(this);
	}

}
