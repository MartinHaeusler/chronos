package org.chronos.chronosphere.impl.query.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class TraversalBaseSource<S, E> implements TraversalSource<S, E> {

    private final ChronoSphereTransactionInternal tx;
    private final Function<Graph, GraphTraversal<S, E>> generator;

    public TraversalBaseSource(ChronoSphereTransactionInternal tx, Function<Graph, GraphTraversal<S, E>> generator) {
        checkNotNull(generator, "Precondition violation - argument 'generator' must not be NULL!");
        this.tx = tx;
        this.generator = generator;
    }

    @Override
    public GraphTraversal<S, E> createTraversal() {
        return this.generator.apply(this.tx.getGraph());
    }

    public ChronoSphereTransactionInternal getTransaction() {
        return this.tx;
    }

}
