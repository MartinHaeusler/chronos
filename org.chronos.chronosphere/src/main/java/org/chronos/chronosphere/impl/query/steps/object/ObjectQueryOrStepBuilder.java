package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import static com.google.common.base.Preconditions.*;
import static org.chronos.chronosphere.impl.query.QueryUtils.*;

public class ObjectQueryOrStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    private final QueryStepBuilder<E, ?>[] subqueries;

    public ObjectQueryOrStepBuilder(final TraversalChainElement previous, final QueryStepBuilder<E, ?>... subqueries) {
        super(previous);
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        this.subqueries = subqueries;
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        return traversal.or(subQueriesToObjectTraversals(tx, this.subqueries, false));
    }
}
