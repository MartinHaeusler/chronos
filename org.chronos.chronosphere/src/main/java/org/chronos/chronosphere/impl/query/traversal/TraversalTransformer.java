package org.chronos.chronosphere.impl.query.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

@FunctionalInterface
public interface TraversalTransformer<S, I, E> extends TraversalChainElement {

    public GraphTraversal<S, E> transformTraversal(ChronoSphereTransactionInternal tx, GraphTraversal<S, I> traversal);

    public static <S, E> TraversalTransformer<S, E, E> identity() {
        return (tx, traversal) -> traversal;
    }
}
