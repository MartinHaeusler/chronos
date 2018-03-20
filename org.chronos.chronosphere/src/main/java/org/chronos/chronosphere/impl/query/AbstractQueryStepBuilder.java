package org.chronos.chronosphere.impl.query;

import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilderInternal;
import org.chronos.chronosphere.api.query.UntypedQueryStepBuilder;
import org.chronos.chronosphere.impl.query.steps.object.*;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.impl.query.traversal.TraversalTransformer;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractQueryStepBuilder<S, I, E> implements QueryStepBuilderInternal<S, E>, TraversalTransformer<S, I, E> {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    protected TraversalChainElement previous;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    protected AbstractQueryStepBuilder(final TraversalChainElement previous) {
        checkNotNull(previous, "Precondition violation - argument 'previous' must not be NULL!");
        this.previous = previous;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public QueryStepBuilder<S, E> filter(final Predicate<E> predicate) {
        checkNotNull(predicate, "Precondition violation - argument 'predicate' must not be NULL!");
        return new ObjectQueryFilterStepBuilder<>(this, predicate);
    }

    @Override
    public QueryStepBuilder<S, E> limit(final long limit) {
        checkArgument(limit >= 0, "Precondition violation - argument 'limit' must not be negative!");
        return new ObjectQueryLimitStepBuilder<>(this, limit);
    }

    @Override
    public QueryStepBuilder<S, E> orderBy(final Comparator<E> comparator) {
        checkNotNull(comparator, "Precondition violation - argument 'comparator' must not be NULL!");
        return new ObjectQueryOrderByStepBuilder<>(this, comparator);
    }

    @Override
    public QueryStepBuilder<S, E> distinct() {
        return new ObjectQueryDistinctStepBuilder<>(this);
    }

    @Override
    public <T> UntypedQueryStepBuilder<S, T> map(final Function<E, T> function) {
        checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
        return new ObjectQueryMapStepBuilder<>(this, function);
    }

    @Override
    public <T> UntypedQueryStepBuilder<S, T> flatMap(final Function<E, Iterator<T>> function) {
        checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
        return new ObjectQueryFlatMapStepBuilder<>(this, function);
    }

    @Override
    public QueryStepBuilder<S, E> notNull() {
        return this.filter(Objects::nonNull);
    }

    @Override
    public QueryStepBuilder<S, E> named(final String stepName) {
        checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
        return new ObjectQueryNamedStepBuilder<>(this, stepName);
    }

    @Override
    public UntypedQueryStepBuilder<S, Object> back(final String stepName) {
        checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
        return new ObjectQueryBackStepBuilder<>(this, stepName);
    }

    @Override
    public QueryStepBuilder<S, E> except(final String stepName) {
        checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
        return new ObjectQueryExceptNamedStepBuilder<>(this, stepName);
    }

    @Override
    public QueryStepBuilder<S, E> except(final Set<?> elementsToExclude) {
        checkNotNull(elementsToExclude, "Precondition violation - argument 'elementsToExclude' must not be NULL!");
        return new ObjectQueryExceptSetStepBuilder<>(this, elementsToExclude);
    }

    @Override
    public UntypedQueryStepBuilder<S, Object> union(final QueryStepBuilder<E, ?>... subqueries) {
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        checkArgument(subqueries.length > 0, "Precondition violation - argument 'subqueries' must not be an empty array!");
        return new ObjectQueryUnionStepBuilder<>(this, subqueries);
    }


    @Override
    public QueryStepBuilder<S, E> and(final QueryStepBuilder<E, ?>... subqueries) {
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        checkArgument(subqueries.length > 0,
            "Precondition violation - argument 'subqueries' must not be an empty array!");
        return new ObjectQueryAndStepBuilder<>(this, subqueries);
    }

    @Override
    public QueryStepBuilder<S, E> or(final QueryStepBuilder<E, ?>... subqueries) {
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        checkArgument(subqueries.length > 0, "Precondition violation - argument 'subqueries' must not be an empty array!");
        return new ObjectQueryOrStepBuilder<>(this, subqueries);
    }

    @Override
    public QueryStepBuilder<S, E> not(final QueryStepBuilder<E, ?> subquery) {
        checkNotNull(subquery, "Precondition violation - argument 'subquery' must not be NULL!");
        return new ObjectQueryNotStepBuilder<>(this, subquery);
    }

    @Override
    public Set<E> toSet() {
        return QueryUtils.prepareTerminalOperation(this, true).toSet();
    }

    @Override
    public List<E> toList() {
        return QueryUtils.prepareTerminalOperation(this, true).toList();
    }

    @Override
    public Stream<E> toStream() {
        return QueryUtils.prepareTerminalOperation(this, true).toStream();
    }

    @Override
    public long count() {
        return QueryUtils.prepareTerminalOperation(this, true).count().next();
    }

    @Override
    public Iterator<E> toIterator() {
        // "toIterator()" does not exist in gremlin; the query itself is
        // the iterator, therefore we do not have a real "terminating" operation here.
        return QueryUtils.prepareTerminalOperation(this, true);
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
}
