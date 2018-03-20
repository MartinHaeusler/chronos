package org.chronos.chronosphere.impl.query.steps.eobject;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;

public class EObjectQueryAllReferencingEObjectsQueryStep<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    public EObjectQueryAllReferencingEObjectsQueryStep(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        return traversal.in().has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString());
    }
}
