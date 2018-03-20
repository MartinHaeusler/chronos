package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class ObjectQueryLimitStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    private final long limit;

    public ObjectQueryLimitStepBuilder(final TraversalChainElement previous, long limit) {
        super(previous);
        this.limit = limit;
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        return traversal.limit(this.limit);
    }
}
