package org.chronos.chronosphere.impl.query.steps.object;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryExceptSetStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, E, E> {

    private final Set<?> set;

    public ObjectQueryExceptSetStepBuilder(final TraversalChainElement previous, Set<?> set) {
        super(previous);
        checkNotNull(set, "Precondition violation - argument 'set' must not be NULL!");
        this.set = Sets.newHashSet(set);
    }

    @Override
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, E> traversal) {
        return traversal.filter(t -> this.set.contains(t.get()) == false);
    }

}
