package org.chronos.chronosphere.impl.query.steps.object;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.impl.query.ObjectQueryStepBuilderImpl;
import org.chronos.chronosphere.impl.query.QueryUtils;
import org.chronos.chronosphere.impl.query.traversal.TraversalChainElement;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.eclipse.emf.ecore.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class ObjectQueryEGetByNameStepBuilder<S> extends ObjectQueryStepBuilderImpl<S, Vertex, Object> {

    private final String eStructuralFeatureName;

    public ObjectQueryEGetByNameStepBuilder(final TraversalChainElement previous, String eStructuralFeatureName) {
        super(previous);
        checkNotNull(eStructuralFeatureName, "Precondition violation - argument 'eStructuralFeatureName' must not be NULL!");
        this.eStructuralFeatureName = eStructuralFeatureName;
    }


    @Override
    public GraphTraversal<S, Object> transformTraversal(final ChronoSphereTransactionInternal tx, final GraphTraversal<S, Vertex> traversal) {
        return traversal.flatMap(this.eGetByName(this.eStructuralFeatureName, tx));
    }

    @SuppressWarnings("unchecked")
    private <E2> Function<Traverser<Vertex>, Iterator<E2>> eGetByName(final String eStructuralFeatureName, final ChronoSphereTransactionInternal tx) {
        return traverser -> {
            if (traverser == null || traverser.get() == null) {
                // skip NULL objects
                return Collections.emptyIterator();
            }
            Vertex vertex = traverser.get();
            String eClassID = (String) vertex.property(ChronoSphereGraphFormat.V_PROP__ECLASS_ID).orElse(null);
            if (eClassID == null) {
                // EClass is unknown? Can this even happen?
                return Collections.emptyIterator();
            }
            ChronoEPackageRegistry registry = tx.getEPackageRegistry();
            EClass eClass = registry.getEClassByID(eClassID);
            if (eClass == null) {
                // EClass with the given ID doesn't exist...
                return Collections.emptyIterator();
            }
            EStructuralFeature feature = eClass.getEStructuralFeature(eStructuralFeatureName);
            if (feature == null) {
                return Collections.emptyIterator();
            }
            if (feature instanceof EAttribute) {
                // the feature we deal with is an EAttribute
                EAttribute eAttribute = (EAttribute) feature;
                String key = ChronoSphereGraphFormat.createVertexPropertyKey(registry, eAttribute);
                Object value = vertex.property(key).orElse(null);
                if (value == null) {
                    return Collections.emptyIterator();
                } else if (value instanceof Collection) {
                    return ((Collection<E2>) value).iterator();
                } else {
                    return Iterators.singletonIterator((E2) value);
                }
            } else if (feature instanceof EReference) {
                // the feature we deal with is an EReference
                EReference eReference = (EReference) feature;
                String label = ChronoSphereGraphFormat.createReferenceEdgeLabel(registry, eReference);
                Iterator<Vertex> targets = vertex.vertices(org.apache.tinkerpop.gremlin.structure.Direction.OUT, label);
                // the next step in the query is certainly not an EObjectQueryStepBuilder, so
                // we have to reify the EObjects.
                Iterator<EObject> eObjectIterator = Iterators.transform(targets, v -> QueryUtils.mapVertexToEObject(tx, v));
                return (Iterator<E2>) eObjectIterator;
            } else {
                throw new IllegalStateException("Unknown subclass of EStructuralFeature: " + feature.getClass().getName());
            }
        };
    }
}
