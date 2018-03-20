package org.chronos.chronosphere.impl.query.steps.eobject;

import com.google.common.base.Objects;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.EObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;

import static com.google.common.base.Preconditions.*;

public class EObjectQueryHasFeatureValueStepBuilder<S> extends EObjectQueryStepBuilderImpl<S, Vertex> {

    private final EStructuralFeature feature;
    private final Object value;

    public EObjectQueryHasFeatureValueStepBuilder(final TraversalChainElement previous, EStructuralFeature feature, Object value) {
        super(previous);
        checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
        this.feature = feature;
        this.value = value;
    }

    @Override
    public GraphTraversal<S, Vertex> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        ChronoEPackageRegistry registry = tx.getEPackageRegistry();
        EStructuralFeature storedFeature = registry.getRegisteredEStructuralFeature(this.feature);
        if (storedFeature instanceof EAttribute) {
            // for EAttributes, we can use what's in the graph directly
            String key = ChronoSphereGraphFormat.createVertexPropertyKey(registry, (EAttribute) storedFeature);
            return traversal.has(key, this.value);
        } else {
            // it's an EReference.
            return traversal
                // we need to reify the EObject...
                .map(t -> QueryUtils.mapVertexToEObject(tx, t))
                // ...  check the condition...
                .filter(t -> {
                    EObject eObject = t.get();
                    if (eObject == null) {
                        return false;
                    }
                    EClass eClass = eObject.eClass();
                    if (eClass.getEAllStructuralFeatures().contains(storedFeature) == false) {
                        // EClass doesn't support this feature
                        return false;
                    }
                    return Objects.equal(eObject.eGet(storedFeature), this.value);
                })
                // ... and transform back
                .map(t -> QueryUtils.mapEObjectToVertex(tx.getGraph(), t.get()));
        }
    }
}
