package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class ObjectQueryDistinctStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    public ObjectQueryDistinctStepBuilder(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        return traversal.dedup();
    }
}
