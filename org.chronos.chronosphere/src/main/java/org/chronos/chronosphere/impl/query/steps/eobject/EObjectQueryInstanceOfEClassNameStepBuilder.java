package org.chronos.chronosphere.impl.query.steps.eobject;

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

public class EObjectQueryInstanceOfEClassNameStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final String eClassName;
    private final boolean allowSubclasses;

    public EObjectQueryInstanceOfEClassNameStepBuilder(final TraversalChainElement previous, String eClassName, boolean allowSubclasses) {
        super(previous);
        checkNotNull(eClassName, "Precondition violation - argument 'eClassName' must not be NULL!");
        this.eClassName = eClassName;
        this.allowSubclasses = allowSubclasses;
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        // try to find the EClass by qualified name
        EClass eClass = tx.getEClassByQualifiedName(this.eClassName);
        if (eClass == null) {
            // try again, by simple name
            eClass = tx.getEClassBySimpleName(this.eClassName);
        }
        if (eClass == null) {
            throw new IllegalArgumentException("Could not find EClass with name '" + this.eClassName + "'!");
        }
        EClass finalClass = eClass;
        if (this.allowSubclasses == false) {
            String eClassID = tx.getEPackageRegistry().getEClassID(finalClass);
            String key = ChronoSphereGraphFormat.V_PROP__ECLASS_ID;
            return traversal.has(key, eClassID);
        } else {
            // we want to include subclasses, we don't have much choice other
            // than resolving the EObjects and using the Ecore API. Optimization?
            return traversal
                // we need to reify the EObject...
                .map(t -> QueryUtils.mapVertexToEObject(tx, t))
                // ...  check the condition...
                .filter(t -> {
                    EObject eObject = t.get();
                    if (eObject == null) {
                        return false;
                    }
                    return finalClass.isInstance(eObject);
                })
                // ... and transform back
                .map(t -> QueryUtils.mapEObjectToVertex(tx.getGraph(), t.get()));
        }
    }
}
