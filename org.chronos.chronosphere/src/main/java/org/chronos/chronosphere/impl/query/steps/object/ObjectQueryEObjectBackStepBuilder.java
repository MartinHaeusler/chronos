package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryEObjectBackStepBuilder<S> extends ObjectQueryStepBuilderImpl<S, Vertex, Object> {

    private final String stepName;

    public ObjectQueryEObjectBackStepBuilder(final TraversalChainElement previous, final String stepName) {
        super(previous);
        checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
        this.stepName = stepName;
    }

    @Override
    public GraphTraversal<S, Object> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        return traversal.select(this.stepName);
    }
}
