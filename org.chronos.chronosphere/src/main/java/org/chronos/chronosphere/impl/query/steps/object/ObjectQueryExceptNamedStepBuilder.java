package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryExceptNamedStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    private final String stepName;

    public ObjectQueryExceptNamedStepBuilder(final TraversalChainElement previous, final String stepName) {
        super(previous);
        checkNotNull(stepName, "Precondition violation - argument 'stepName' must not be NULL!");
        this.stepName = stepName;
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        // syntax according to: https://groups.google.com/d/msg/gremlin-users/EZUU00UEdoY/nX11hMu4AgAJ
        return traversal.where(P.neq(this.stepName));
    }
}
