package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.api.query.QueryStepBuilder;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import static com.google.common.base.Preconditions.*;
import static org.chronos.chronosphere.impl.query.QueryUtils.*;

public class ObjectQueryNotStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    private final QueryStepBuilder<E, ?> subquery;

    public ObjectQueryNotStepBuilder(final TraversalChainElement previous, final QueryStepBuilder<E, ?> subquery) {
        super(previous);
        checkNotNull(subquery, "Precondition violation - argument 'subquery' must not be NULL!");
        this.subquery = subquery;
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        GraphTraversal innerTraversal = resolveTraversalChain(this.subquery, tx, false);
        return traversal.not(innerTraversal);
    }
}
