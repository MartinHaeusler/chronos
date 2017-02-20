package org.chronos.chronograph.internal.impl.structure.graph.proxy;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;

public class ChronoVertexProxy extends AbstractElementProxy<ChronoVertexImpl> implements ChronoVertex {

	public ChronoVertexProxy(final ChronoGraph graph, final String id) {
		super(graph, id);
	}

	public ChronoVertexProxy(final ChronoVertexImpl vertex) {
		super(vertex.graph(), vertex.id());
		this.element = vertex;
	}

	// =================================================================================================================
	// TINKERPOP API
	// =================================================================================================================

	@Override
	public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
		return this.getElement().addEdge(label, inVertex, keyValues);
	}

	@Override
	public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
		return this.getElement().properties(propertyKeys);
	}

	@Override
	public <V> VertexProperty<V> property(final Cardinality cardinality, final String key, final V value, final Object... keyValues) {
		return this.getElement().property(cardinality, key, value, keyValues);
	}

	@Override
	public <V> VertexProperty<V> property(final String key, final V value) {
		return this.getElement().property(key, value);
	}

	@Override
	public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
		return this.getElement().property(key, value, keyValues);
	}

	@Override
	public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
		return this.getElement().edges(direction, edgeLabels);
	}

	@Override
	public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
		return this.getElement().vertices(direction, edgeLabels);
	}

	@Override
	public String toString() {
		return StringFactory.vertexString(this);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	protected ChronoVertexImpl loadElement(final ChronoGraphTransaction tx, final String id) {
		ChronoVertex vertex = (ChronoVertex) tx.getVertex(id);
		return ChronoProxyUtil.resolveVertexProxy(vertex);
	}

	@Override
	protected void registerProxyAtTransaction(final ChronoGraphTransaction transaction) {
		transaction.getContext().registerVertexProxyInCache(this);
	}

}
