package org.chronos.chronosphere.impl.query.steps.eobject;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.eclipse.emf.ecore.EReference;

import java.util.List;

import static com.google.common.base.Preconditions.*;

public class EObjectQueryEGetReferenceStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final EReference eReference;

    public EObjectQueryEGetReferenceStepBuilder(final TraversalChainElement previous, EReference eReference) {
        super(previous);
        checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
        this.eReference = eReference;
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        ChronoEPackageRegistry registry = tx.getEPackageRegistry();
        String edgeLabel = ChronoSphereGraphFormat.createReferenceEdgeLabel(registry, this.eReference);
        if (this.eReference.isOrdered()) {
            return traversal.flatMap(traverser -> {
                Vertex vertex = traverser.get();
                if (this.eReference.isMany()) {
                    List<Vertex> eReferenceTargets = ChronoSphereGraphFormat.getEReferenceTargets(registry, vertex, this.eReference);
                    return eReferenceTargets.iterator();
                } else {
                    Vertex target = ChronoSphereGraphFormat.getEReferenceTarget(registry, vertex, this.eReference);
                    return Iterators.singletonIterator(target);
                }
            });
        } else {
            return traversal.out(edgeLabel);
        }
    }

}
