package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import java.util.Comparator;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryOrderByStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    private final Comparator<E> comparator;

    public ObjectQueryOrderByStepBuilder(final TraversalChainElement previous, final Comparator<E> comparator) {
        super(previous);
        checkNotNull(comparator, "Precondition violation - argument 'comparator' must not be NULL!");
        this.comparator = comparator;
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        return traversal.order().by(this.comparator);
    }
}
