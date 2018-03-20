package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryFilterStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    private final Predicate<E> predicate;

    public ObjectQueryFilterStepBuilder(final TraversalChainElement previous, Predicate<E> predicate) {
        super(previous);
        checkNotNull(predicate, "Precondition violation - argument 'predicate' must not be NULL!");
        this.predicate = predicate;
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        return traversal.filter(traverser -> {
            E object = traverser.get();
            return this.predicate.test(object);
        });
    }

}
