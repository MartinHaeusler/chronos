package org.chronos.chronosphere.impl.query;

import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.api.query.*;
import org.chronos.chronosphere.impl.query.steps.eobject.*;
import org.chronos.chronosphere.impl.query.steps.object.*;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.impl.query.traversal.TraversalTransformer;
import org.eclipse.emf.ecore.*;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.*;

public abstract class EObjectQueryStepBuilderImpl<S, I> implements EObjectQueryStepBuilder<S>, QueryStepBuilderInternal<S, EObject>, TraversalTransformer<S, I, Vertex> {


    private TraversalChainElement previous;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================


    public EObjectQueryStepBuilderImpl(final TraversalChainElement previous) {
        this.previous = previous;
    }

    @Override
    public EObjectQueryStepBuilder<S> orderBy(final Comparator<EObject> comparator) {
        checkNotNull(comparator, "Precondition violation - argument 'comparator' must not be NULL!");
        ObjectQueryEObjectReifyStepBuilder<Object> reified = new ObjectQueryEObjectReifyStepBuilder<>(this);
        ObjectQueryOrderByStepBuilder<Object, EObject> ordered = new ObjectQueryOrderByStepBuilder<>(reified, comparator);
        return new EObjectQueryAsEObjectStepBuilder<>(ordered);
    }

    @Override
    public EObjectQueryStepBuilder<S> orderBy(final EAttribute eAttribute, final Order order) {
        checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        // TODO this could be optimized by implementing the orderBy() on graph-level rather than
        // resolving the actual EObjects.
        return this.orderBy(new EObjectAttributeComparator(eAttribute, order));
    }

    @Override
    public EObjectQueryStepBuilder<S> distinct() {
        return new EObjectQueryDistinctStepBuilder<>(this);
    }

    @Override
    public <T> UntypedQueryStepBuilder<S, T> map(final Function<EObject, T> function) {
        checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
        // the map function is specified on EObject API level, so we first have to transform
        // our vertices into EObjects
        ObjectQueryEObjectReifyStepBuilder<Object> reified = new ObjectQueryEObjectReifyStepBuilder<>(this);
        return new ObjectQueryMapStepBuilder<>(reified, function);
    }

    @Override
    public <T> UntypedQueryStepBuilder<S, T> flatMap(final Function<EObject, Iterator<T>> function) {
        checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
        // the map function is specified on EObject API level, so we first have to transform
        // our vertices into EObjects
        ObjectQueryEObjectReifyStepBuilder<Object> reified = new ObjectQueryEObjectReifyStepBuilder<>(this);
        return new ObjectQueryFlatMapStepBuilder<>(reified, function);
    }

    @Override
    public Set<EObject> toSet() {
        QueryStepBuilderInternal<S, EObject> finalStep = this.reifyEObjects();
        GraphTraversal<S, EObject> traversal = QueryUtils.prepareTerminalOperation(finalStep, true);
        return traversal.toSet();
    }


    @Override
    public List<EObject> toList() {
        QueryStepBuilderInternal<S, EObject> finalStep = this.reifyEObjects();
        GraphTraversal<S, EObject> traversal = QueryUtils.prepareTerminalOperation(finalStep, true);
        return traversal.toList();
    }

    @Override
    public Iterator<EObject> toIterator() {
        QueryStepBuilderInternal<S, EObject> finalStep = this.reifyEObjects();
        GraphTraversal<S, EObject> traversal = QueryUtils.prepareTerminalOperation(finalStep, true);
        return traversal;
    }

    @Override
    public Stream<EObject> toStream() {
        QueryStepBuilderInternal<S, EObject> finalStep = this.reifyEObjects();
        GraphTraversal<S, EObject> traversal = QueryUtils.prepareTerminalOperation(finalStep, true);
        return traversal.toStream();
    }

    @Override
    public long count() {
        GraphTraversal<S, EObject> traversal = QueryUtils.prepareTerminalOperation(this, false);
        return traversal.count().next();
    }

    @Override
    public EObjectQueryStepBuilder<S> limit(final long limit) {
        checkArgument(limit >= 0, "Precondition violation - argument 'limit' must not be negative!");
        return new EObjectQueryLimitStepBuilder<>(this, limit);
    }

    @Override
    public EObjectQueryStepBuilder<S> notNull() {
        return new EObjectQueryNonNullStepBuilder<>(this);
    }

    @Override
    public EObjectQueryStepBuilder<S> has(final String eStructuralFeatureName, final Object value) {
        checkNotNull(eStructuralFeatureName,
            "Precondition violation - argument 'eStructuralFeatureName' must not be NULL!");
        // we have to apply this on the EObject API, sadly
        ObjectQueryEObjectReifyStepBuilder<Object> reified = new ObjectQueryEObjectReifyStepBuilder<>(this);
        ObjectQueryFilterStepBuilder<Object, EObject> filtered = new ObjectQueryFilterStepBuilder<>(reified, (eObject -> {
            if (eObject == null) {
                return false;
            }
            EClass eClass = eObject.eClass();
            EStructuralFeature feature = eClass.getEStructuralFeature(eStructuralFeatureName);
            if (feature == null) {
                return false;
            }

            // DON'T DO THIS! This will break EAttributes with enum types that have a default
            // value even when they are not set!
            // if (eObject.eIsSet(feature) == false) {
            // return false;
            // }

            return Objects.equal(eObject.eGet(feature), value);
        }));
        return new EObjectQueryAsEObjectStepBuilder<>(filtered);
    }

    @Override
    public EObjectQueryStepBuilder<S> has(final EStructuralFeature eStructuralFeature, final Object value) {
        checkNotNull(eStructuralFeature, "Precondition violation - argument 'eStructuralFeature' must not be NULL!");
        return new EObjectQueryHasFeatureValueStepBuilder<>(this, eStructuralFeature, value);
    }

    @Override
    public EObjectQueryStepBuilder<S> isInstanceOf(final EClass eClass) {
        checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
        return this.isInstanceOf(eClass, true);
    }

    @Override
    public EObjectQueryStepBuilder<S> isInstanceOf(final EClass eClass, final boolean allowSubclasses) {
        checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
        return new EObjectQueryInstanceOfEClassStepBuilder<>(this, eClass, allowSubclasses);
    }

    @Override
    public EObjectQueryStepBuilder<S> isInstanceOf(final String eClassName) {
        checkNotNull(eClassName, "Precondition violation - argument 'eClassName' must not be NULL!");
        return this.isInstanceOf(eClassName, true);
    }

    @Override
    public EObjectQueryStepBuilder<S> isInstanceOf(final String eClassName, final boolean allowSubclasses) {
        checkNotNull(eClassName, "Precondition violation - argument 'eClassName' must not be NULL!");
        return new EObjectQueryInstanceOfEClassNameStepBuilder<>(this, eClassName, allowSubclasses);
    }

    @Override
    @SuppressWarnings("unchecked")
    public UntypedQueryStepBuilder<S, Object> eGet(final String eStructuralFeatureName) {
        checkNotNull(eStructuralFeatureName,
            "Precondition violation - argument 'eStructuralFeatureName' must not be NULL!");
        return new ObjectQueryEGetByNameStepBuilder<>(this, eStructuralFeatureName);
    }


    @Override
    public EObjectQueryStepBuilder<S> eGet(final EReference eReference) {
        checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
        return new EObjectQueryEGetReferenceStepBuilder<>(this, eReference);
    }

    @Override
    public EObjectQueryStepBuilder<S> eGetInverse(final EReference eReference) {
        checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
        return new EObjectQueryEGetInverseByReferenceStepBuilder<>(this, eReference);
    }

    @Override
    public EObjectQueryStepBuilder<S> eGetInverse(final String referenceName) {
        checkNotNull(referenceName, "Precondition violation - argument 'referenceName' must not be NULL!");
        return new EObjectQueryEGetInverseByNameStepBuilder<>(this, referenceName);
    }

    @Override
    public UntypedQueryStepBuilder<S, Object> eGet(final EAttribute eAttribute) {
        checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
        return new ObjectQueryEGetAttributeStepBuilder<>(this, eAttribute);
    }


    @Override
    public EObjectQueryStepBuilder<S> eContainer() {
        return new EObjectQueryEContainerStepBuilder<>(this);
    }

    @Override
    public EObjectQueryStepBuilder<S> eContents() {
        return new EObjectQueryEContentsStepBuilder<>(this);
    }

    @Override
    public EObjectQueryStepBuilder<S> eAllContents() {
        return new EObjectQueryEAllContentsStepBuilder<>(this);
    }

    @Override
    public EObjectQueryStepBuilder<S> allReferencingEObjects() {
        return new EObjectQueryAllReferencingEObjectsQueryStep<>(this);
    }

    @Override
    public EObjectQueryStepBuilder<S> named(final String name) {
        checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
        // the content of the named steps are reified EObjects, not vertices. We therefore
        // have to transform our vertices into EObjects, apply the except step, and then transform
        // back into Vertex representation.
        ObjectQueryEObjectReifyStepBuilder<Object> reified = new ObjectQueryEObjectReifyStepBuilder<>(this);
        ObjectQueryNamedStepBuilder<Object, EObject> except = new ObjectQueryNamedStepBuilder<>(reified, name);
        return new EObjectQueryAsEObjectStepBuilder<>(except);
    }

    @Override
    public UntypedQueryStepBuilder<S, Object> back(final String stepName) {
        checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
        return new ObjectQueryEObjectBackStepBuilder<>(this, stepName);
    }

    @Override
    public EObjectQueryStepBuilder<S> except(final String stepName) {
        checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
        // the content of the named steps are reified EObjects, not vertices. We therefore
        // have to transform our vertices into EObjects, apply the except step, and then transform
        // back into Vertex representation.
        ObjectQueryEObjectReifyStepBuilder<Object> reified = new ObjectQueryEObjectReifyStepBuilder<>(this);
        ObjectQueryExceptNamedStepBuilder<Object, EObject> except = new ObjectQueryExceptNamedStepBuilder<>(reified, stepName);
        return new EObjectQueryAsEObjectStepBuilder<>(except);
    }

    @Override
    public EObjectQueryStepBuilder<S> except(final Set<?> elementsToExclude) {
        checkNotNull(elementsToExclude, "Precondition violation - argument 'elementsToExclude' must not be NULL!");
        return new EObjectQueryExceptObjectsStepBuilder<>(this, elementsToExclude);
    }

    @Override
    public UntypedQueryStepBuilder<S, Object> union(final QueryStepBuilder<EObject, ?>... subqueries) {
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        checkArgument(subqueries.length > 0, "Precondition violation - argument 'subqueries' must not be an empty array!");
        return new ObjectQueryEObjectUnionStepBuilder<>(this, subqueries);
    }

    @Override
    public EObjectQueryStepBuilder<S> and(final QueryStepBuilder<EObject, ?>... subqueries) {
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        checkArgument(subqueries.length > 0, "Precondition violation - argument 'subqueries' must not be an empty array!");
        return new EObjectQueryAndStepBuilder<>(this, subqueries);
    }

    @Override
    public EObjectQueryStepBuilder<S> or(final QueryStepBuilder<EObject, ?>... subqueries) {
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        checkArgument(subqueries.length > 0, "Precondition violation - argument 'subqueries' must not be an empty array!");
        return new EObjectQueryOrStepBuilder<>(this, subqueries);
    }


    @Override
    public EObjectQueryStepBuilder<S> not(final QueryStepBuilder<EObject, ?> subquery) {
        checkNotNull(subquery, "Precondition violation - argument 'subquery' must not be NULL!");
        return new EObjectQueryNotStepBuilder<>(this, subquery);
    }

    @Override
    public EObjectQueryStepBuilder<S> filter(final Predicate<EObject> predicate) {
        checkNotNull(predicate, "Precondition violation - argument 'predicate' must not be NULL!");
        // the predicate is formulated based on the EObject API. We are dealing with vertices
        // here internally, so we first have to reify the EObject, test the predicate, and then
        // transform the remaining EObjects back into Vertex representation, because the next
        // query step expects vertices as input again.
        ObjectQueryEObjectReifyStepBuilder<Object> reified = new ObjectQueryEObjectReifyStepBuilder<>(this);
        ObjectQueryFilterStepBuilder<Object, EObject> filtered = new ObjectQueryFilterStepBuilder<>(reified, predicate);
        return new EObjectQueryAsEObjectStepBuilder<>(filtered);
    }

    @Override
    public EObjectQueryStepBuilder<S> closure(final EReference eReference, Direction direction) {
        checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
        checkNotNull(direction, "Precondition violation - argument 'direction' must not be NULL!");
        return new EObjectQueryClosureStepBuilder<>(this, eReference, direction);
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    @Override
    public TraversalChainElement getPrevious() {
        return this.previous;
    }

    @Override
    public void setPrevious(final TraversalChainElement previous) {
        checkNotNull(previous, "Precondition violation - argument 'previous' must not be NULL!");
        this.previous = previous;
    }

    // =====================================================================================================================
    // HELPER METHODS
    // =====================================================================================================================

    protected QueryStepBuilderInternal<S, EObject> reifyEObjects() {
        return new ObjectQueryEObjectReifyStepBuilder<>(this);
    }


}
