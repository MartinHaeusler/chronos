package org.chronos.chronograph.api.builder.query;

import java.util.Iterator;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

/**
 * Defines the last step in the fluent graph query builder API.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <E>
 *            Either {@link Vertex} or {@link Edge}, depending on the result type of this query.
 */
public interface GraphQueryBuilderFinalizer<E extends Element> {

	// =================================================================================================================
	// BASIC API
	// =================================================================================================================

	/**
	 * Returns the result of this query as an {@link Iterator} over the resulting elements.
	 *
	 * @return The iterator. May be empty, but never <code>null</code>.
	 */
	public Iterator<E> toIterator();

	/**
	 * Takes the elements resulting in this query and feeds them into a new {@link GraphTraversal}.
	 *
	 * @return A {@link GraphTraversal} containing the result elements of this query as start elements. Never <code>null</code>.
	 */
	public GraphTraversal<E, E> toTraversal();

	// =================================================================================================================
	// EXTENDED API (utility methods)
	// =================================================================================================================

	/**
	 * Collects the resulting elements of this query in a {@link Set} and returns it.
	 *
	 * <p>
	 * Please note that this method eagerly fetches all resulting elements. In many cases, using {@link #toIterator()} can yield better performance and/or lower memory consumption.
	 *
	 * @return The resulting elements of this query in a {@link Set}.
	 */
	public default Set<E> toSet() {
		Iterator<E> iterator = this.toIterator();
		Set<E> resultSet = Sets.newHashSet();
		while (iterator.hasNext()) {
			E v = iterator.next();
			resultSet.add(v);
		}
		return resultSet;
	}

	/**
	 * Returns the {@link Element#id() id}s of the elements resulting from this query and returns them as an {@link Iterator}.
	 *
	 * @return An iterator over the resulting element IDs. Never <code>null</code>, may be empty.
	 */
	public default Iterator<String> toIdIterator() {
		return Iterators.transform(this.toIterator(), element -> (String) element.id());
	}

	/**
	 * Collects the {@link Element#id() id}s of the resulting elements of this query in a {@link Set} and returns it.
	 *
	 * <p>
	 * Please note that this method eagerly fetches all resulting elements. In many cases, using {@link #toIdIterator()} can yield better performance and/or lower memory consumption.
	 *
	 * @return The ids of the resulting elements of this query in a {@link Set}.
	 */
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
