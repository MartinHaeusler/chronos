package org.chronos.chronograph.api.builder.query;

import java.util.Iterator;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public interface GraphQueryBuilderFinalizer<E extends Element> {

	// =================================================================================================================
	// BASIC API
	// =================================================================================================================

	public Iterator<E> toIterator();

	public GraphTraversal<E, E> toTraversal();

	// =================================================================================================================
	// EXTENDED API (utility methods)
	// =================================================================================================================

	public default Set<E> toSet() {
		Iterator<E> iterator = this.toIterator();
		Set<E> resultSet = Sets.newHashSet();
		while (iterator.hasNext()) {
			E v = iterator.next();
			resultSet.add(v);
		}
		return resultSet;
	}

	public default Iterator<String> toIdIterator() {
		return Iterators.transform(this.toIterator(), element -> (String) element.id());
	}

	public default Set<String> toIdSet() {
		Iterator<E> iterator = this.toIterator();
		Set<String> resultSet = Sets.newHashSet();
		while (iterator.hasNext()) {
			E e = iterator.next();
			resultSet.add((String) e.id());
		}
		return resultSet;
	}

}
