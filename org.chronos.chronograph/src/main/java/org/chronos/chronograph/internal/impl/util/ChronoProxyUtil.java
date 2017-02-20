package org.chronos.chronograph.internal.impl.util;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.AbstractElementProxy;

import com.google.common.collect.Iterators;

public class ChronoProxyUtil {

	public static <T extends ChronoElement> T resolveProxy(final T maybeProxy) {
		if (maybeProxy == null) {
			return null;
		}
		if (maybeProxy instanceof AbstractElementProxy) {
			// proxy, resolve
			@SuppressWarnings("unchecked")
			AbstractElementProxy<T> proxy = (AbstractElementProxy<T>) maybeProxy;
			return proxy.getElement();
		} else {
			// not a proxy
			return maybeProxy;
		}
	}

	public static ChronoVertexImpl resolveVertexProxy(final Vertex vertex) {
		return (ChronoVertexImpl) resolveProxy((ChronoVertex) vertex);
	}

	public static ChronoEdgeImpl resolveEdgeProxy(final Edge edge) {
		return (ChronoEdgeImpl) resolveProxy((ChronoEdge) edge);
	}

	public static Iterator<Vertex> replaceVerticesByProxies(final Iterator<Vertex> iterator,
			final ChronoGraphTransaction tx) {
		checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return Iterators.transform(iterator, v -> tx.getContext().getOrCreateVertexProxy(v));
	}

	public static Iterator<Edge> replaceEdgesByProxies(final Iterator<Edge> iterator, final ChronoGraphTransaction tx) {
		checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		return Iterators.transform(iterator, e -> tx.getContext().getOrCreateEdgeProxy(e));
	}

}
