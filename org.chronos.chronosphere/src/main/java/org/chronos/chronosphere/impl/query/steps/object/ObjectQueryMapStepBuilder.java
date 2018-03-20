package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryMapStepBuilder<S, I, E> extends ObjectQueryStepBuilderImpl<S, I, E> {

    private final Function<I, E> function;

    public ObjectQueryMapStepBuilder(final TraversalChainElement previous, final Function<I, E> function) {
        super(previous);
        checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
        this.function = function;
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, I> traversal) {
        return traversal.map(t -> this.function.apply(t.get()));
    }
}
