package org.chronos.chronograph.internal.impl.util;

import static com.google.common.base.Preconditions.*;

import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;

public class ChronoTraversalUtil {

	/**
	 * Returns the {@link ChronoGraph} on which the given {@link Traversal} is executed.
	 *
	 * <p>
	 * This method assumes that the traversal is indeed executed on a ChronoGraph. If that is not the case, an
	 * {@link IllegalArgumentException} is thrown.
	 *
	 * @param traversal
	 *            The traversal to get the underlying ChronoGraph for. Must not be <code>null</code>.
	 *
	 * @return The ChronoGraph on which the given traversal is executed. Never <code>null</code>.
	 */
	public static ChronoGraph getChronoGraph(final Traversal<?, ?> traversal) {
		checkNotNull(traversal, "Precondition violation - argument 'traversal' must not be NULL!");
		Optional<Graph> optGraph = TraversalHelper.getRootTraversal(traversal.asAdmin()).getGraph();
		if (optGraph.isPresent() == false) {
			throw new IllegalArgumentException("Traversal is not bound to a graph: " + traversal);
		}
		Graph graph = optGraph.get();
		if (graph instanceof ChronoGraph == false) {
			throw new IllegalArgumentException(
					"Traversal is not bound to a ChronoGraph, but a '" + graph.getClass().getName() + "'!");
		}
		return (ChronoGraph) graph;
	}

	/**
	 * Returns the {@link ChronoGraphTransaction} on which the given {@link Traversal} is executed.
	 *
	 * <p>
	 * This method assumes that the traversal is indeed executed on a ChronoGraph. If that is not the case, an
	 * {@link IllegalArgumentException} is thrown.
	 *
	 *
	 * @param traversal
	 *            The traversal to get the underlying transaction for. Must not be <code>null</code>.
	 * @return The underlying chrono graph transaction. Never <code>null</code>.
	 */
	public static ChronoGraphTransaction getTransaction(final Traversal<?, ?> traversal) {
		checkNotNull(traversal, "Precondition violation - argument 'traversal' must not be NULL!");
		ChronoGraph g = getChronoGraph(traversal);
		return g.tx().getCurrentTransaction();
	}

}
