package org.chronos.chronograph.internal.impl.optimizer.step;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.impl.query.DoubleSearchSpecificationImpl;
import org.chronos.chronodb.internal.impl.query.LongSearchSpecificationImpl;
import org.chronos.chronodb.internal.impl.query.StringSearchSpecificationImpl;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.impl.util.ChronoGraphQueryUtil;
import org.chronos.chronograph.internal.impl.util.ChronoTraversalUtil;
import org.chronos.common.util.ReflectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

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
            ChronoGraphTransactionInternal tx = (ChronoGraphTransactionInternal) graph.tx().getCurrentTransaction();
            // convert the "has" containers that are indexable into search specifications
            Map<HasContainer, SearchSpecification<?>> containerToSearchSpec = this.getSearchSpecifications();
            Iterator<Vertex> vertices = null;
            if (containerToSearchSpec.isEmpty()) {
                // none of the 'has' conditions works based on equality, so none is indexed
                // -> we have to iterate over all vertices
                vertices = graph.vertices();
            } else {
                // at least one of the conditions is based on equality
                // -> pass it to the indexer
                vertices = tx.getVerticesBySearchSpecifications(containerToSearchSpec.values());
            }
            // in order to handle all conditions which are not based on Gremlin's "Compare" class, we
            // post-process the vertices by filtering them once more with these conditions
            List<HasContainer> nonIndexedHasContainers = this.getAllContainersExcept(containerToSearchSpec.keySet());
            if (nonIndexedHasContainers.isEmpty()) {
                // there are no non-indexed containers, we can return the iterator directly
                return vertices;
            } else {
                // we have some non-indexed "has" containers, apply them as a post-processing
                return Iterators.filter(vertices, v -> HasContainer.testAll(v, nonIndexedHasContainers));
            }
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
            ChronoGraphTransactionInternal tx = (ChronoGraphTransactionInternal) graph.tx().getCurrentTransaction();
            // convert the "has" containers that are indexable into search specifications
            Map<HasContainer, SearchSpecification<?>> containerToSearchSpec = this.getSearchSpecifications();
            Iterator<Edge> edges = null;
            if (containerToSearchSpec.isEmpty()) {
                // none of the 'has' conditions works based on equality, so none is indexed
                // -> we have to iterate over all edges
                edges = graph.edges();
            } else {
                // at least one of the conditions is based on equality
                // -> pass it to the indexer
                edges = tx.getEdgesBySearchSpecifications(containerToSearchSpec.values());
            }
            // in order to handle all conditions which are not based on Gremlin's "Compare" class, we
            // post-process the vertices by filtering them once more with these conditions
            List<HasContainer> nonIndexedHasContainers = this.getAllContainersExcept(containerToSearchSpec.keySet());
            return Iterators.filter(edges, e -> HasContainer.testAll(e, nonIndexedHasContainers));
        }
    }

    private Map<HasContainer, SearchSpecification<?>> getSearchSpecifications() {
        Map<HasContainer, SearchSpecification<?>> resultMap = Maps.newLinkedHashMap();
        for (HasContainer container : this.hasContainers) {
            SearchSpecification<?> searchSpec = hasContainerToSearchSpec(container);
            if (searchSpec != null) {
                resultMap.put(container, searchSpec);
            }
        }
        return resultMap;
    }

    private List<HasContainer> getAllContainersExcept(final Collection<HasContainer> excludedContainers) {
        return this.hasContainers.stream().filter(c -> excludedContainers.contains(c) == false)
            .collect(Collectors.toList());
    }

    private static SearchSpecification<?> hasContainerToSearchSpec(final HasContainer container) {
        String property = container.getKey();
        Object value = container.getValue();
        if (value == null) {
            throw new IllegalArgumentException("NULL values are not allowed in has(...) steps.");
        }
        if (container.getBiPredicate() instanceof Compare == false) {
            // non-standard conditions cannot be mapped (must be iterated linearly)
            return null;
        }
        if (Compare.eq.equals(container.getBiPredicate())) {
            if (value instanceof String) {
                String searchVal = (String) value;
                return new StringSearchSpecificationImpl(property, Condition.EQUALS, searchVal, TextMatchMode.STRICT);
            } else if (ReflectionUtils.isLongCompatible(value)) {
                long searchVal = ReflectionUtils.asLong(value);
                return new LongSearchSpecificationImpl(property, Condition.EQUALS, searchVal);
            } else if (ReflectionUtils.isDoubleCompatible(value)) {
                double searchVal = ReflectionUtils.asDouble(value);
                double tolerance = 10e-6; // a very small tolerance to avoid rounding issues on double equality
                return new DoubleSearchSpecificationImpl(property, Condition.EQUALS, searchVal, tolerance);
            } else {
                // we are checking equality, but the argument is not of an indexable type; we can't use an index
                // query in this case (regular iteration and comparison is required).
                return null;
            }
        } else if (Compare.neq.equals(container.getBiPredicate())) {
            if (value instanceof String) {
                String searchVal = (String) value;
                return new StringSearchSpecificationImpl(property, Condition.NOT_EQUALS, searchVal,
                    TextMatchMode.STRICT);
            } else if (ReflectionUtils.isLongCompatible(value)) {
                long searchVal = ReflectionUtils.asLong(value);
                return new LongSearchSpecificationImpl(property, Condition.NOT_EQUALS, searchVal);
            } else if (ReflectionUtils.isDoubleCompatible(value)) {
                double searchVal = ReflectionUtils.asDouble(value);
                double tolerance = 10e-6; // a very small tolerance to avoid rounding issues on double equality
                return new DoubleSearchSpecificationImpl(property, Condition.NOT_EQUALS, searchVal, tolerance);
            } else {
                // we are checking inequality, but the argument is not of an indexable type; we can't use an index
                // query in this case (regular iteration and comparison is required).
                return null;
            }
        } else {
            // this cast is safe, we checked it above.
            Compare compare = (Compare) container.getBiPredicate();
            NumberCondition numberCondition = ChronoGraphQueryUtil.gremlinCompareToNumberCondition(compare);
            if (value instanceof String) {
                throw new IllegalArgumentException("The predicate " + compare
                    + " cannot be used on String values in a has(...) step. Please use numeric values instead.");
            } else if (ReflectionUtils.isLongCompatible(value)) {
                long searchVal = ReflectionUtils.asLong(value);
                return new LongSearchSpecificationImpl(property, numberCondition, searchVal);
            } else if (ReflectionUtils.isDoubleCompatible(value)) {
                double searchVal = ReflectionUtils.asDouble(value);
                return new DoubleSearchSpecificationImpl(property, numberCondition, searchVal, 0);
            } else {
                // we are checking a predicate of an unknown type; we can't use an index.
                return null;
            }
        }
    }

}
