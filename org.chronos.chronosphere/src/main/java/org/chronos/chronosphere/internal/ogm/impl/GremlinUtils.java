package org.chronos.chronosphere.internal.ogm.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class GremlinUtils {

	/**
	 * Makes sure that only a single outgoing {@link Edge} with the given <code>label</code> exists in the
	 * <code>source</code> {@link Vertex} and that edge connects to the <code>target</code> {@link Vertex}.
	 *
	 * <p>
	 * There are several cases:
	 * <ol>
	 * <li>There is no outgoing edge at the source vertex with the given label. In this case, a new edge will be added
	 * from source to target vertex with the given label.
	 * <li>There is an outgoing edge at the source vertex with the given label, and that edge connects to the given
	 * target vertex. In this case, this method is a no-op and simply returns that edge.
	 * <li>There is an outgoing edge at the source vertex with the given label, and that edge connects to a vertex other
	 * than the given target vertex. In this case, this old edge is removed, a new edge is added from the given source
	 * vertex to the given target vertex with the given label, and the new edge is returned.
	 * </ol>
	 *
	 * @param source
	 *            The source vertex that owns the edge to override. Must not be <code>null</code>.
	 * @param label
	 *            The label of the single edge. Must not be <code>null</code>.
	 * @param target
	 *            The target vertex to which the single edge should connect. Must not be <code>null</code>.
	 * @return The single outgoing edge in the source vertex with the given label that connects to the given target
	 *         vertex. The edge may have been created, may have already existed, or may have been redirected from a
	 *         previous target (see specification above).
	 */
	public static Edge setEdgeTarget(final Vertex source, final String label, final Vertex target) {
		checkNotNull(source, "Precondition violation - argument 'source' must not be NULL!");
		checkNotNull(target, "Precondition violation - argument 'target' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		Set<Edge> edges = Sets.newHashSet(source.edges(Direction.OUT, label));
		if (edges.isEmpty()) {
			// there is no existing outgoing edge with the given label at the source vertex.
			// We can "override" by simply adding a new edge to the desired target vertex with our label.
			return source.addEdge(label, target);
		} else {
			// there is at least one outgoing edge from the source verex wiht our label. Check where it leads to.
			Edge edgeToKeep = null;
			for (Edge edge : edges) {
				if (edge.inVertex().equals(target)) {
					// this is the correct edge; we can keep it.
					edgeToKeep = edge;
				} else {
					// this edge leads to the wrong target, remove it
					edge.remove();
				}
			}
			if (edgeToKeep != null) {
				// we have found an edge that leads to the desired target, no need to override
				return edgeToKeep;
			} else {
				// all previous edges with the given label lead to other vertices. We removed them all,
				// so now we need to create the edge to the desired target.
				return source.addEdge(label, target);
			}
		}
	}

	/**
	 * Makes sure that every {@linkplain Edge edge} with the given <code>label</code> that is outgoing from the given
	 * <code>source</code> {@linkplain Vertex vertex} leads to one of the given <code>targets</code>, and that there is
	 * one edge for each target.
	 *
	 * <p>
	 * This method is a two-step process.
	 * <ol>
	 * <li>In the <code>source</code> vertex, iterate over the outgoing edges that have the given <code>label</code>.
	 * <ol>
	 * <li>If the current edge leads to a vertex that is in our <code>targets</code> set, keep the edge and add it to
	 * the result set.
	 * <li>if the current edge leads to a vertex that is not in our <code>targets</code> set, delete it.
	 * </ol>
	 * <li>For each vertex in our <code>targets</code> that we did not find in the previous step, create a new edge from
	 * the <code>source</code> vertex with the given label, and add it to the result set.
	 * </ol>
	 *
	 * @param source
	 *            The source vertex that owns the edge to override. Must not be <code>null</code>.
	 * @param label
	 *            The edge label in question. Must not be <code>null</code>.
	 * @param targets
	 *            The desired set of target vertices. May be empty (in which case all outgoing edges from the source
	 *            vertex will be deleted and the result set will be empty), but must not be <code>null</code>.
	 * @return The set of edges that lead from the source vertex to one of the target vertices via the given label. The
	 *         edges may have already existed, or were generated by this method. May be empty in case that the
	 *         <code>targets</code> set was empty, but will never be <code>null</code>.
	 */
	public static Set<Edge> setEdgeTargets(final Vertex source, final String label, final Set<Vertex> targets) {
		checkNotNull(source, "Precondition violation - argument 'source' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		checkNotNull(targets, "Precondition violation - argument 'targets' must not be NULL!");
		Set<Edge> resultSet = Sets.newHashSet();
		Set<Vertex> verticesToConnectTo = Sets.newHashSet(targets);
		Set<Edge> existingEdges = Sets.newHashSet(source.edges(Direction.OUT, label));
		for (Edge edge : existingEdges) {
			Vertex target = edge.inVertex();
			if (targets.contains(target)) {
				// we are already connected with this vertex
				verticesToConnectTo.remove(target);
				resultSet.add(edge);
			} else {
				// this edge is no longer required, remove it
				edge.remove();
			}
		}
		// in case that we have target vertices to which we are not already connectd, add these edges now
		for (Vertex target : verticesToConnectTo) {
			Edge newEdge = source.addEdge(label, target);
			resultSet.add(newEdge);
		}
		return resultSet;
	}

	/**
	 * A variant of {@link #setEdgeTargets(Vertex, String, Set)} that respects target ordering and non-uniqueness of
	 * targets.
	 *
	 * <p>
	 * This method will check if there is an {@link Edge} from the given <code>source</code> {@link Vertex} to each of
	 * the given <code>target</code> vertices. If the list of targets contains one vertex several times, this method
	 * will ensure that there are as many edges to that target. Any outgoing edge from the given <code>source</code>
	 * with the given <code>label</code> that is <b>not</b> reused by this process will be deleted. Additional edges
	 * will be created only if necessary. The returned list of edges will be ordered by target vertex, and have the same
	 * ordering as the given list of <code>targets</code>.
	 *
	 * @param source
	 *            The source vertex that owns the edge to override. Must not be <code>null</code>.
	 * @param label
	 *            The edge label in question. Must not be <code>null</code>.
	 * @param targets
	 *            The desired list of target vertices. May be empty (in which case all outgoing edges from the source
	 *            vertex will be deleted and the result set will be empty), but must not be <code>null</code>. Multiple
	 *            occurrences of the same vertex will be respected, as well as the ordering of vertices in the list.
	 * @return The list of edges that lead from the source vertex to one of the target vertices via the given label. The
	 *         edges may have already existed, or were generated by this method. May be empty in case that the
	 *         <code>targets</code> set was empty, but will never be <code>null</code>.
	 */
	public static List<Edge> setEdgeTargets(final Vertex source, final String label, final List<Vertex> targets) {
		checkNotNull(source, "Precondition violation - argument 'source' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		checkNotNull(targets, "Precondition violation - argument 'targets' must not be NULL!");
		List<Edge> resultList = Lists.newArrayList();
		Set<Edge> existingEdges = Sets.newHashSet(source.edges(Direction.OUT, label));
		for (Vertex targetVertex : targets) {
			Optional<Edge> maybeEdge = existingEdges.stream().filter(edge -> edge.inVertex().equals(targetVertex))
					.findAny();
			if (maybeEdge.isPresent()) {
				Edge edge = maybeEdge.get();
				// we reuse this edge
				resultList.add(edge);
				// ... and make sure that we don't reuse it twice
				existingEdges.remove(edge);
			} else {
				// we don't have an edge to this target; add one
				resultList.add(source.addEdge(label, targetVertex));
			}
		}
		// all edges that remain are "unused" and therefore need to be deleted
		existingEdges.forEach(edge -> edge.remove());
		return resultList;
	}

	/**
	 * Finds and returns the single {@link Edge} that connects the given <code>source</code> {@link Vertex} with the
	 * given <code>target</code> {@link Vertex}.
	 *
	 * @param source
	 *            The source vertex of the edge to look for. Must not be <code>null</code>.
	 * @param label
	 *            The label of the edge to look for. Must not be <code>null</code>.
	 * @param target
	 *            The target vertex of the edge to look for. Must not be <code>null</code>.
	 * @return The single egde that connects the given source and target vertices with the given label, or
	 *         <code>null</code> if no such edge exists.
	 *
	 * @throws IllegalArgumentException
	 *             Thrown if there is more than one edge with the given label from the given source to the given target
	 *             vertex.
	 */
	public static Edge getEdge(final Vertex source, final String label, final Vertex target) {
		Set<Edge> edges = getEdges(source, label, target);
		if (edges.isEmpty()) {
			return null;
		}
		if (edges.size() > 1) {
			throw new IllegalArgumentException("Multiple edges (" + edges.size() + ") with label '" + label
					+ "'connect the source vertex " + source + " with target vertex " + target + "!");
		}
		return Iterables.getOnlyElement(edges);
	}

	/**
	 * Finds and returns the {@link Edge}s that connects the given <code>source</code> {@link Vertex} with the given
	 * <code>target</code> {@link Vertex}.
	 *
	 * @param source
	 *            The source vertex of the edge to look for. Must not be <code>null</code>.
	 * @param label
	 *            The label of the edge to look for. Must not be <code>null</code>.
	 * @param target
	 *            The target vertex of the edge to look for. Must not be <code>null</code>.
	 * @return The edges that connects the given source and target vertices with the given label. May be empty if no
	 *         edge is found, but never <code>null</code>.
	 */
	public static Set<Edge> getEdges(final Vertex source, final String label, final Vertex target) {
		checkNotNull(source, "Precondition violation - argument 'source' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		checkNotNull(target, "Precondition violation - argument 'target' must not be NULL!");
		Set<Edge> resultSet = Sets.newHashSet();
		source.edges(Direction.OUT, label).forEachRemaining(edge -> {
			if (edge.inVertex().equals(target)) {
				resultSet.add(edge);
			}
		});
		return resultSet;
	}

	/**
	 * Searches for an {@link Edge} between the given <code>source</code> and <code>target</code> {@linkplain Vertex
	 * vertices} and returns it, creating a new one if it does not exist.
	 *
	 * @param source
	 *            The source vertex. Must not be <code>null</code>.
	 * @param label
	 *            The edge label. Must not be <code>null</code>.
	 * @param target
	 *            The target vertex. Must not be <code>null</code>.
	 *
	 * @return The edge. If it already existed, it will be returned untouched, otherwise a new edge was created and will
	 *         be returned.
	 */
	public static Edge ensureEdgeExists(final Vertex source, final String label, final Vertex target) {
		checkNotNull(source, "Precondition violation - argument 'source' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		checkNotNull(target, "Precondition violation - argument 'target' must not be NULL!");
		Iterator<Edge> edges = source.edges(Direction.OUT, label);
		while (edges.hasNext()) {
			Edge edge = edges.next();
			if (edge.inVertex().equals(target)) {
				// found it
				return edge;
			}
		}
		// edge not found; create it
		return source.addEdge(label, target);
	}

}
