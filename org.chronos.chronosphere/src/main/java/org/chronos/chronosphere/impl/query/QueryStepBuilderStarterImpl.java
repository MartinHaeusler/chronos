package org.chronos.chronosphere.impl.query;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.api.query.EObjectQueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilderStarter;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.chronos.chronosphere.impl.query.steps.eobject.EObjectQueryIdentityStepBuilder;
import org.chronos.chronosphere.impl.query.traversal.TraversalBaseSource;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public class QueryStepBuilderStarterImpl implements QueryStepBuilderStarter {

    private ChronoSphereTransactionInternal owningTransaction;

    public QueryStepBuilderStarterImpl(final ChronoSphereTransactionInternal owningTransaction) {
        checkNotNull(owningTransaction, "Precondition violation - argument 'owningTransaction' must not be NULL!");
        this.owningTransaction = owningTransaction;
    }

    @Override
    public EObjectQueryStepBuilder<EObject> startingFromAllEObjects() {
        TraversalBaseSource<Vertex, Vertex> source = new TraversalBaseSource<>
            (this.owningTransaction,
                g -> g.traversal()
                    // start with all vertices
                    .V()
                    // restrict to those of kind "EObject"
                    .has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString())
            );
        return this.createEQueryStepBuilderFromTraversalSource(source);
    }


    @Override
    public EObjectQueryStepBuilder<EObject> startingFromInstancesOf(final EClass eClass) {
        checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
        String id = this.owningTransaction.getEPackageRegistry().getEClassID(eClass);
        TraversalBaseSource<Vertex, Vertex> source = new TraversalBaseSource<>
            (this.owningTransaction,
                g -> g.traversal()
                    // start with all vertices
                    .V()
                    // restrict to the instances of the given eclass
                    .has(ChronoSphereGraphFormat.V_PROP__ECLASS_ID, id)
                    // restrict to EObjects only
                    .has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString())
            );
        return this.createEQueryStepBuilderFromTraversalSource(source);
    }

    @Override
    public EObjectQueryStepBuilder<EObject> startingFromInstancesOf(final String eClassName) {
        checkNotNull(eClassName, "Precondition violation - argument 'eClassName' must not be NULL!");
        // try to get the EClass via qualified name
        EClass eClass = this.owningTransaction.getEClassByQualifiedName(eClassName);
        if (eClass == null) {
            // try to get it via simple name
            eClass = this.owningTransaction.getEClassBySimpleName(eClassName);
        }
        return this.startingFromInstancesOf(eClass);
    }

    @Override
    public EObjectQueryStepBuilder<EObject> startingFromEObjectsWith(final EAttribute attribute, final Object value) {
        checkNotNull(attribute, "Precondition violation - argument 'attribute' must not be NULL!");
        ChronoEPackageRegistry registry = this.owningTransaction.getEPackageRegistry();
        String vertexPropertyKey = ChronoSphereGraphFormat.createVertexPropertyKey(registry, attribute);
        TraversalBaseSource<Vertex, Vertex> source = new TraversalBaseSource<>
            (this.owningTransaction, g -> g.traversal()
                // start with all vertices
                .V()
                // restrict to EObject vertices only
                .has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString())
                // restrict to EObject vertices that have the given attribute set to the given value
                .has(vertexPropertyKey, value)
            );
        return this.createEQueryStepBuilderFromTraversalSource(source);
    }

    @Override
    public EObjectQueryStepBuilder<EObject> startingFromEObject(final EObject eObject) {
        checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
        ChronoEObjectInternal chronoEObject = (ChronoEObjectInternal) eObject;
        if (chronoEObject.isAttached() == false) {
            throw new IllegalArgumentException(
                "The given EObject is not attached to the repository - cannot start a query from it!");
        }
        TraversalBaseSource<Vertex, Vertex> source = new TraversalBaseSource<>
            (this.owningTransaction, g -> g.traversal()
                // start the traversal from the vertex that represents the EObject
                .V(chronoEObject.getId())
            );
        return this.createEQueryStepBuilderFromTraversalSource(source);
    }

    @Override
    public EObjectQueryStepBuilder<EObject> startingFromEObjects(final Iterable<? extends EObject> eObjects) {
        checkNotNull(eObjects, "Precondition violation - argument 'eObjects' must not be NULL!");
        Iterable<? extends EObject> filteredEObjects = Iterables.filter(eObjects, eObject -> {
            ChronoEObjectInternal chronoEObject = (ChronoEObjectInternal) eObject;
            if (chronoEObject.isAttached() == false) {
                throw new IllegalArgumentException(
                    "One of the given EObjects is not attached to the repository - cannot start a query from them!");
            }
            return chronoEObject.exists();
        });
        List<String> ids = Lists.newArrayList();
        filteredEObjects.forEach(eObject -> {
            ids.add(((ChronoEObject) eObject).getId());
        });
        String[] idsArray = ids.toArray(new String[ids.size()]);
        TraversalBaseSource<Vertex, Vertex> source;
        if (ids.isEmpty()) {
            // neither of the given EObjects exists, or the iterable has been empty
            source = new TraversalBaseSource<>(this.owningTransaction, g ->
                // start at a random UUID to enforce an empty traversal
                g.traversal().V(UUID.randomUUID())
            );
        } else {
            // start from the given IDs
            source = new TraversalBaseSource<>(this.owningTransaction, g ->
                g.traversal().V(idsArray)
            );
        }
        return this.createEQueryStepBuilderFromTraversalSource(source);
    }

    @Override
    public EObjectQueryStepBuilder<EObject> startingFromEObjects(final Iterator<? extends EObject> eObjects) {
        checkNotNull(eObjects, "Precondition violation - argument 'eObjects' must not be NULL!");
        List<EObject> list = Lists.newArrayList(eObjects);
        return this.startingFromEObjects(list);
    }

    // =====================================================================================================================
    // HELPER METHODS
    // =====================================================================================================================

    private EObjectQueryStepBuilder<EObject> createEQueryStepBuilderFromTraversalSource(final TraversalBaseSource<Vertex, Vertex> source) {
        return new EObjectQueryIdentityStepBuilder<>(source);
    }

}
