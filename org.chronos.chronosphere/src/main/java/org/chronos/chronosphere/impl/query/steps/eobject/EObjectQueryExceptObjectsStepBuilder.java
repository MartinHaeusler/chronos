package org.chronos.chronosphere.impl.query.steps.eobject;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;

import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class EObjectQueryExceptObjectsStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {


    private final Set<String> eObjectIDsToExclude;

    public EObjectQueryExceptObjectsStepBuilder(final TraversalChainElement previous, final Set<?> elementsToExclude) {
        super(previous);
        checkNotNull(elementsToExclude, "Precondition violation - argument 'elementsToExclude' must not be NULL!");
        this.eObjectIDsToExclude = Sets.newHashSet();
        for (Object element : elementsToExclude) {
            if (element instanceof ChronoEObject == false) {
                // ignore this object
                continue;
            }
            ChronoEObject cEObject = (ChronoEObject) element;
            this.eObjectIDsToExclude.add(cEObject.getId());
        }
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        return traversal.filter(t -> {
            Vertex vertex = t.get();
            if (vertex == null) {
                return false;
            }
            return this.eObjectIDsToExclude.contains(vertex.id()) == false;
        });
    }
}
