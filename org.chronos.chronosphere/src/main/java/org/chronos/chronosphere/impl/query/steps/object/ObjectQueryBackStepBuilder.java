package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryBackStepBuilder<S, I> extends ObjectQueryStepBuilderImpl<S, I, Object> {

    private final String name;

    public ObjectQueryBackStepBuilder(final TraversalChainElement previous, String name) {
        super(previous);
        checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
        this.name = name;
    }

    @Override
    public GraphTraversal<S, Object> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, I> traversal) {
        return traversal.select(this.name);
    }
}
