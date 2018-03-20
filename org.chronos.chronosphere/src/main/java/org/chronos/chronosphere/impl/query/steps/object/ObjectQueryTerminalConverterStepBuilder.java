package org.chronos.chronosphere.impl.query.steps.object;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

public class ObjectQueryTerminalConverterStepBuilder<S, E> extends ObjectQueryStepBuilderImpl<S, Object, E> {

    public ObjectQueryTerminalConverterStepBuilder(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    @SuppressWarnings("unchecked")
    public GraphTraversal<S, E> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Object> traversal) {
        // convert all vertices to EObjects, but leave everything else alone.
        GraphTraversal<S, Object> newTraversal = traversal.map(traverser -> {
            Object element = traverser.get();
            if (element instanceof Vertex == false) {
                return element;
            }
            Vertex vertex = (Vertex) element;
            return QueryUtils.mapVertexToEObject(tx, vertex);
        });
        return (GraphTraversal<S, E>) newTraversal;
    }
}
