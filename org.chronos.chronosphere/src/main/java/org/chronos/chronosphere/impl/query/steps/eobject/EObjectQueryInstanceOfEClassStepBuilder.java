package org.chronos.chronosphere.impl.query.steps.eobject;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

import static com.google.common.base.Preconditions.*;

public class EObjectQueryInstanceOfEClassStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final EClass eClass;
    private final boolean allowSubclasses;

    public EObjectQueryInstanceOfEClassStepBuilder(final TraversalChainElement previous, EClass eClass, boolean allowSubclasses) {
        super(previous);
        checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
        this.eClass = eClass;
        this.allowSubclasses = allowSubclasses;
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        if (this.allowSubclasses == false) {
            String eClassID = tx.getEPackageRegistry().getEClassID(this.eClass);
            String key = ChronoSphereGraphFormat.V_PROP__ECLASS_ID;
            return traversal.has(key, eClassID);
        } else {
            // we want to include subclasses, we don't have much choice other
            // than resolving the EObjects and using the Ecore API. Optimization?
            return traversal
                .map(t -> QueryUtils.mapVertexToEObject(tx, t))
                .filter(this::filterEObject)
                .map(t -> QueryUtils.mapEObjectToVertex(tx.getGraph(), t.get()));
        }
    }

    private boolean filterEObject(Traverser<EObject> traverser) {
        EObject eObject = traverser.get();
        if (eObject == null) {
            return false;
        }
        return this.eClass.isSuperTypeOf(eObject.eClass());
    }
}
