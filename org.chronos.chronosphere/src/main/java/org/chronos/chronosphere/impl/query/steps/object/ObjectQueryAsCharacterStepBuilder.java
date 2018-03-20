package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class ObjectQueryAsCharacterStepBuilder<S> extends ObjectQueryStepBuilderImpl<S, Object, Character> {

    public ObjectQueryAsCharacterStepBuilder(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public GraphTraversal<S, Character> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Object> traversal) {
        return QueryUtils.castTraversalTo(traversal, Character.class);
    }
}
