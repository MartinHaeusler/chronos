package org.chronos.chronograph.internal.impl.optimizer.step;

import static com.google.common.base.Preconditions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.util.ChronoTraversalUtil;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public class ChronoGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

	private final List<HasContainer> hasContainers = new ArrayList<>();

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronoGraphStep(final GraphStep<S, E> originalStep) {
		super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(),
				originalStep.getIds());
		// copy the labels of the original step
		originalStep.getLabels().forEach(this::addLabel);
		// set the result iterator supplier function (i.e. the function that calculates the result of this step)
		this.setIteratorSupplier(this::getResultIterator);
	}

	// =====================================================================================================================
	// TINKERPOP API
	// =====================================================================================================================

	@Override
	public List<HasContainer> getHasContainers() {
		return Collections.unmodifiableList(this.hasContainers);
	}

	// =====================================================================================================================
	// ADDITIONAL PUBLIC API
	// =====================================================================================================================

	@Override
	public void addHasContainer(final HasContainer container) {
		checkNotNull(container, "Precondition violation - argument 'container' must not be NULL!");
		this.hasContainers.add(container);
	}

	@Override
	public String toString() {
		// according to TinkerGraph reference implementation
		if (this.hasContainers.isEmpty()) {
			return super.toString();
		} else {
			return 0 == this.ids.length
					? StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers)
					: StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(),
							Arrays.toString(this.ids), this.hasContainers);
		}
	}

	// =====================================================================================================================
	// ITERATION & STEP RESULT CALCULATION
	// =====================================================================================================================

	@SuppressWarnings("unchecked")
	private Iterator<E> getResultIterator() {
		if (Vertex.class.isAssignableFrom(this.returnClass)) {
			return (Iterator<E>) this.getResultVertices();
		} else {
			return (Iterator<E>) this.getResultEdges();
		}
	}

	private Iterator<Vertex> getResultVertices() {
		ChronoGraph graph = ChronoTraversalUtil.getChronoGraph(this.getTraversal());
		if (this.ids != null && this.ids.length > 0) {
			// we have input vertex IDs to work with (as in: graph.V(id1,id2).has('name','martin') )
			// -> get the vertices for the IDs and filter them
			Iterator<Vertex> vertices = graph.vertices(this.ids);
			// keep only the vertices which pass all of our 'has' conditions
			return Iterators.filter(vertices, v -> HasContainer.testAll(v, this.hasContainers));
		} else {
			// we have no input vertex IDs to work with (as in: graph.V().has('name','martin') )
			graph.tx().readWrite();
			ChronoGraphTransaction tx = graph.tx().getCurrentTransaction();
			// convert the "has" containers that are based on equality into a map
			Map<String, String> propertyKeyToSearchValue = this.getEqualityConditions();
			Iterator<Vertex> vertices = null;
			if (propertyKeyToSearchValue.isEmpty()) {
				// none of the 'has' conditions works based on equality, so none is indexed
				// -> we have to iterate over all vertices
				vertices = graph.vertices();
			} else {
				// at least one of the conditions is based on equality
				// -> pass it to the indexer
				vertices = tx.getVerticesByProperties(propertyKeyToSearchValue);
			}
			// in order to handle all 'has' conditions which are not based on equality, we
			// post-process the vertices by filtering them once more with these conditions
			List<HasContainer> nonEqualityHasConditions = this.getNonEqualityBasedHasContainers();
			return Iterators.filter(vertices, v -> HasContainer.testAll(v, nonEqualityHasConditions));
		}
	}

	private Iterator<Edge> getResultEdges() {
		ChronoGraph graph = ChronoTraversalUtil.getChronoGraph(this.getTraversal());
		if (this.ids != null && this.ids.length > 0) {
			// we have input edge IDs to work with (as in: graph.E(id1,id2).has('since','2004') )
			// -> get the edges for the IDs and filter them
			Iterator<Edge> edges = graph.edges(this.ids);
			// keep only the edges which pass all of our 'has' conditions
			return Iterators.filter(edges, e -> HasContainer.testAll(e, this.hasContainers));
		} else {
			// we have no input edge IDs to work with (as in: graph.E().has('since','2004') )
			graph.tx().readWrite();
			ChronoGraphTransaction tx = graph.tx().getCurrentTransaction();
			// convert the "has" containers that are based on equality into a map
			Map<String, String> propertyKeyToSearchValue = this.getEqualityConditions();
			Iterator<Edge> edges = null;
			if (propertyKeyToSearchValue.isEmpty()) {
				// none of the 'has' conditions works based on equality, so none is indexed
				// -> we have to iterate over all edges
				edges = graph.edges();
			} else {
				// at least one of the conditions is based on equality
				// -> pass it to the indexer
				edges = tx.getEdgesByProperties(propertyKeyToSearchValue);
			}
			// in order to handle all 'has' conditions which are not based on equality, we
			// post-process the edges by filtering them once more with these conditions
			List<HasContainer> nonEqualityHasConditions = this.getNonEqualityBasedHasContainers();
			return Iterators.filter(edges, e -> HasContainer.testAll(e, nonEqualityHasConditions));
		}
	}

	private Map<String, String> getEqualityConditions() {
		Map<String, String> propertyKeyToSearchValue = Maps.newHashMap();
		for (HasContainer container : this.hasContainers) {
			if (container.getBiPredicate() != Compare.eq) {
				// we are concerned only about equalities
				continue;
			}
			String propertyKey = container.getKey();
			String searchValue = String.valueOf(container.getValue());
			propertyKeyToSearchValue.put(propertyKey, searchValue);
		}
		return propertyKeyToSearchValue;
	}

	private List<HasContainer> getNonEqualityBasedHasContainers() {
		return this.hasContainers.stream().filter(has -> has.getBiPredicate() != Compare.eq)
				.collect(Collectors.toList());
	}

}
