package org.chronos.chronosphere.impl.query.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

@FunctionalInterface
public interface TraversalSource<S, E> extends TraversalChainElement {

    public GraphTraversal<S, E> createTraversal();

    public static <S, E> TraversalSource<S, E> createAnonymousSource() {
        return () -> (GraphTraversal<S, E>) __.identity();
    }
}
