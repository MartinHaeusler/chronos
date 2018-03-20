package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class ObjectQueryAsBooleanStepBuilder<S> extends ObjectQueryStepBuilderImpl<S, Object, Boolean> {

    public ObjectQueryAsBooleanStepBuilder(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public GraphTraversal<S, Boolean> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Object> traversal) {
        return QueryUtils.castTraversalTo(traversal, Boolean.class);
    }
}
