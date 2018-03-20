package org.chronos.chronosphere.impl.query.steps.eobject;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;
import org.eclipse.emf.ecore.EObject;

import java.util.Collections;

public class EObjectQueryAsEObjectStepBuilder<S, I> extends EObjectQueryStepBuilderImpl<S, I> {

    public EObjectQueryAsEObjectStepBuilder(final TraversalChainElement previous) {
        super(previous);
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, I> traversal) {
        return traversal.flatMap(traverser -> {
            Object value = traverser.get();
            if (value instanceof EObject) {
                // return the vertex that represents the EObject
                return Iterators.singletonIterator(QueryUtils.mapEObjectToVertex(tx, (EObject) value));
            } else if (value instanceof Vertex) {
                Vertex vertex = (Vertex) value;
                VertexKind kind = ChronoSphereGraphFormat.getVertexKind(vertex);
                if (Objects.equal(kind, VertexKind.EOBJECT)) {
                    // this vertex represents an EObject
                    return Iterators.singletonIterator(vertex);
                } else {
                    // whatever this vertex is, it's not an EObject
                    return Collections.emptyIterator();
                }
            } else {
                return Collections.emptyIterator();
            }
        });
    }


}
