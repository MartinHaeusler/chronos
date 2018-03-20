package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import static com.google.common.base.Preconditions.*;
import static org.chronos.chronosphere.impl.query.QueryUtils.*;

public class ObjectQueryUnionStepBuilder<S, I> extends ObjectQueryStepBuilderImpl<S, I, Object> {

    private final QueryStepBuilder<I, ?>[] subqueries;

    @SafeVarargs
    public ObjectQueryUnionStepBuilder(final TraversalChainElement previous, final QueryStepBuilder<I, ?>... subqueries) {
        super(previous);
        checkNotNull(subqueries, "Precondition violation - argument 'subqueries' must not be NULL!");
        this.subqueries = subqueries;
    }

    @Override
    public GraphTraversal<S, Object> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, I> traversal) {
        return traversal.union(subQueriesToObjectTraversals(tx, this.subqueries, true));
    }
}
