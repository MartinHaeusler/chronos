package org.chronos.chronosphere.impl.query.steps.eobject;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class EObjectQueryLimitStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final long limit;

    public EObjectQueryLimitStepBuilder(final TraversalChainElement previous, final long limit) {
        super(previous);
        this.limit = limit;
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        return traversal.limit(this.limit);
    }
}
