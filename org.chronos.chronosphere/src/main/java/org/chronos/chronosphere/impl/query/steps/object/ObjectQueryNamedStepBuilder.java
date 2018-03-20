package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryNamedStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    private final String name;

    public ObjectQueryNamedStepBuilder(final TraversalChainElement previous, String name) {
        super(previous);
        checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
        this.name = name;
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        return traversal.as(this.name);
    }
}
