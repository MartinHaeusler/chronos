package org.chronos.chronosphere.impl.query;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.EmptyGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronosphere.api.query.EObjectQueryStepBuilder;
import org.chronos.chronosphere.api.query.QueryStepBuilderStarter;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class QueryStepBuilderStarterImpl implements QueryStepBuilderStarter {

	private ChronoSphereTransactionInternal owningTransaction;

	public QueryStepBuilderStarterImpl(final ChronoSphereTransactionInternal owningTransaction) {
		checkNotNull(owningTransaction, "Precondition violation - argument 'owningTransaction' must not be NULL!");
		this.owningTransaction = owningTransaction;
	}

	@Override
	public EObjectQueryStepBuilder<EObject> startingFromAllEObjects() {
		GraphTraversal<Vertex, EObject> traversal = this.owningTransaction.getGraph().traversal()
				// start with all vertices
				.V()
				// restrict to those of kind "EObject"
				.has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString())
				// map the vertices to EObjects
				.map(this::mapVertexTraverserToEObject);
		return this.newBuilderFromTraversal(traversal);
	}

	@Override
	public EObjectQueryStepBuilder<EObject> startingFromInstancesOf(final EClass eClass) {
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		GraphTraversal<Vertex, EObject> traversal = this.owningTransaction.getGraph().traversal()
				// start with all vertices
				.V()
				// restrict to the eClasses
				.has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.ECLASS.toString())
				// find the correct one. First check the name...
				.has(ChronoSphereGraphFormat.V_PROP__NAME, eClass.getName())
				// remember this step
				.as("EClass")
				// go to the owning EPackage vertex
				.in(ChronoSphereGraphFormat.E_LABEL__EPACKAGE_OWNED_CLASSIFIERS)
				// make sure that the EPackage is the correct one by checking NSURI, NSPrefix and name
				.has(ChronoSphereGraphFormat.V_PROP__NS_URI, eClass.getEPackage().getNsURI())
				.has(ChronoSphereGraphFormat.V_PROP__NS_PREFIX, eClass.getEPackage().getNsPrefix())
				.has(ChronoSphereGraphFormat.V_PROP__NAME, eClass.getEPackage().getName())
				// after we have asserted that the owning EPackage is the one we are looking for, go back to the EClass
				.select("EClass")
				// go to the EObject instances of that EClass
				.in(ChronoSphereGraphFormat.createEClassReferenceEdgeLabel())
				// map the vertices to EObjects
				.map(this::mapVertexTraverserToEObject);
		return this.newBuilderFromTraversal(traversal);
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
		GraphTraversal<Vertex, EObject> traversal = this.owningTransaction.getGraph().traversal()
				// start with all vertices
				.V()
				// restrict to EObject vertices only
				.has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString())
				// restrict to EObject vertices that have the given attribute set to the given value
				.has(ChronoSphereGraphFormat.createVertexPropertyKey(registry, attribute), value)
				// map the vertices to EObjects
				.map(this::mapVertexTraverserToEObject);
		return this.newBuilderFromTraversal(traversal);
	}

	@Override
	public EObjectQueryStepBuilder<EObject> startingFromEObject(final EObject eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		ChronoEObjectInternal chronoEObject = (ChronoEObjectInternal) eObject;
		if (chronoEObject.isAttached() == false) {
			throw new IllegalArgumentException(
					"The given EObject is not attached to the repository - cannot start a query from it!");
		}
		GraphTraversal<Vertex, EObject> traversal = this.owningTransaction.getGraph().traversal()
				// start the traversal from the vertex that represents the EObject
				.V(chronoEObject.getId())
				// map the vertices to EObjects
				.map(this::mapVertexTraverserToEObject);
		return this.newBuilderFromTraversal(traversal);
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
		GraphTraversal<Vertex, EObject> traversal = null;
		if (ids.isEmpty()) {
			traversal = EmptyGraphTraversal.instance();
		} else {
			traversal = this.owningTransaction.getGraph().traversal()
					// start the traversal from the vertices that represent our EObjects
					.V(ids.toArray())
					// map the vertices to EObjects
					.map(this::mapVertexTraverserToEObject);
		}
		return this.newBuilderFromTraversal(traversal);
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

	private EObjectQueryStepBuilder<EObject> newBuilderFromTraversal(final GraphTraversal<Vertex, EObject> traversal) {
		checkNotNull(traversal, "Precondition violation - argument 'traversal' must not be NULL!");
		EObjectQueryStepBuilderImpl<EObject> builder = new EObjectQueryStepBuilderImpl<>(null, traversal);
		builder.setTransaction(this.owningTransaction);
		return builder;
	}

	private EObject mapVertexToEObject(final Vertex vertex) {
		if (vertex == null) {
			return null;
		}
		EObject eObject = this.owningTransaction.getEObjectById((String) vertex.id());
		return eObject;
	}

	private EObject mapVertexTraverserToEObject(final Traverser<Vertex> traverser) {
		return this.mapVertexToEObject(traverser.get());
	}

}
