package org.chronos.chronosphere.internal.ogm.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.internal.api.SphereTransactionContext;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.EObjectToGraphMapper;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EReference;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class EObjectToGraphMapperImpl implements EObjectToGraphMapper {

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public EObjectToGraphMapperImpl() {

	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Vertex getOrCreatePlainVertexForEObject(final SphereTransactionContext ctx, final ChronoEObject eObject) {
		checkNotNull(ctx, "Precondition violation - argument 'ctx' must not be NULL!");
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		ChronoGraph graph = ctx.getGraph();
		Vertex vertex = Iterators.getOnlyElement(graph.vertices(eObject.getId()), null);
		if (vertex == null) {
			// we don't know that object yet; create a new vertex for it
			vertex = graph.addVertex(T.id, eObject.getId());
			ChronoSphereGraphFormat.setVertexKind(vertex, VertexKind.EOBJECT);
			return vertex;
		} else {
			// the object was already persisted once, we already have a vertex for it
			return vertex;
		}
	}

	@Override
	public void mapAllEObjectPropertiesToGraph(final SphereTransactionContext ctx, final ChronoEObject eObject) {
		checkNotNull(ctx, "Precondition violation - argument 'ctx' must not be NULL!");
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		checkNotNull(eObject.eClass(), "Precondition violation - argument 'eObject' has no 'eClass' assigned!");
		// iterate over the attributes and assign them to the vertex one by one
		for (EAttribute attribute : eObject.eClass().getEAllAttributes()) {
			this.mapEAttributeToGraph(ctx, eObject, attribute);
		}
	}

	@Override
	public void mapAllEObjectReferencesToGraph(final SphereTransactionContext session, final ChronoEObject eObject) {
		checkNotNull(session, "Precondition violation - argument 'ctx' must not be NULL!");
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		checkNotNull(eObject.eClass(), "Precondition violation - argument 'eObject' has no 'eClass' assigned!");
		// make sure that we actually HAVE a vertex for the EObject
		this.getOrCreatePlainVertexForEObject(session, eObject);
		// iterate over the references and assign them to the vertex one by one
		for (EReference reference : eObject.eClass().getEAllReferences()) {
			this.mapEReferenceToGraph(session, eObject, reference);
		}
	}

	@Override
	public void mapEAttributeToGraph(final SphereTransactionContext ctx, final ChronoEObject eObject,
			final EAttribute attribute) {
		checkNotNull(ctx, "Precondition violation - argument 'ctx' must not be NULL!");
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		checkNotNull(attribute, "Precondition violation - argument 'attribute' must not be NULL!");
		// make sure that we actually HAVE a vertex for the EObject
		Vertex vertex = this.getOrCreatePlainVertexForEObject(ctx, eObject);
		ChronoEPackageRegistry cep = ctx.getChronoEPackage();
		// check if we are dealing with a multiplicity-one or a multiplicity-many feature
		if (attribute.isMany()) {
			// multiplicity-many feature
			Collection<?> values = (Collection<?>) eObject.eGet(attribute);
			ChronoSphereGraphFormat.setEAttributeValues(cep, vertex, attribute, values);
		} else {
			// multiplicity-one feature
			Object value = eObject.eGet(attribute);
			ChronoSphereGraphFormat.setEAttributeValue(cep, vertex, attribute, value);
		}
	}

	@Override
	public void mapEReferenceToGraph(final SphereTransactionContext ctx, final ChronoEObject eObject,
			final EReference reference) {
		checkNotNull(ctx, "Precondition violation - argument 'ctx' must not be NULL!");
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		checkNotNull(reference, "Precondition violation - argument 'reference' must not be NULL!");
		ChronoGraph graph = ctx.getGraph();
		// make sure that we actually HAVE a vertex for the EObject
		Vertex vertex = this.getOrCreatePlainVertexForEObject(ctx, eObject);
		// prepare the ChronoEPackage for later use
		ChronoEPackageRegistry cep = ctx.getChronoEPackage();
		// calculate the label for the reference
		String label = ChronoSphereGraphFormat.createReferenceEdgeLabel(cep, reference);
		// check if we are dealing with a multiplicity-one or a multiplicity-many reference
		if (reference.isMany() == false) {
			// get the target of the reference
			ChronoEObject target = (ChronoEObject) eObject.eGet(reference, true);
			Vertex targetVertex = ChronoSphereGraphFormat.getVertexForEObject(graph, target);
			// make sure that an edge exists between the source and target vertices
			GremlinUtils.setEdgeTarget(vertex, label, targetVertex);
		} else {
			// get the targets of the reference
			@SuppressWarnings("unchecked")
			List<ChronoEObject> targets = (List<ChronoEObject>) eObject.eGet(reference, true);
			// FIXME CORRECTNESS: what if the EReference is non-unique!?
			// resolve the target vertices
			Map<ChronoEObject, Vertex> targetEObjectToVertex = Maps.newHashMap();
			for (ChronoEObject target : targets) {
				Vertex targetVertex = ChronoSphereGraphFormat.getVertexForEObject(graph, target);
				targetEObjectToVertex.put(target, targetVertex);
			}
			// set the target vertices
			GremlinUtils.setEdgeTargets(vertex, label, Sets.newHashSet(targetEObjectToVertex.values()));
			if (reference.isOrdered()) {
				// assign ordering properties
				int orderIndex = 0;
				for (ChronoEObject target : targets) {
					// get the vertex that represents this target
					Vertex targetVertex = targetEObjectToVertex.get(target);
					// get the edge between source and target
					Edge edge = GremlinUtils.getEdge(vertex, label, targetVertex);
					// set the order
					ChronoSphereGraphFormat.setEReferenceEdgeOrder(edge, orderIndex);
					orderIndex++;
				}
			}
		}
	}

	@Override
	public void mapEContainerReferenceToGraph(final SphereTransactionContext ctx, final ChronoEObject eObject) {
		checkNotNull(ctx, "Precondition violation - argument 'ctx' must not be NULL!");
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		ChronoGraph graph = ctx.getGraph();
		// make sure that we actually HAVE a vertex for the EObject
		Vertex vertex = this.getOrCreatePlainVertexForEObject(ctx, eObject);
		// get the EContainer
		ChronoEObject eContainer = (ChronoEObject) eObject.eContainer();
		// get the vertex for the eContainer
		Vertex eContainerVertex = ChronoSphereGraphFormat.getVertexForEObject(graph, eContainer);
		// get the label
		String label = ChronoSphereGraphFormat.createEContainerReferenceEdgeLabel();
		// assign the edge target
		GremlinUtils.setEdgeTarget(vertex, label, eContainerVertex);
	}

	@Override
	public void mapEClassReferenceToGraph(final SphereTransactionContext ctx, final ChronoEObject eObject) {
		checkNotNull(ctx, "Precondition violation - argument 'ctx' must not be NULL!");
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		ChronoGraph graph = ctx.getGraph();
		// make sure that we actually HAVE a vertex for the EObject
		Vertex vertex = this.getOrCreatePlainVertexForEObject(ctx, eObject);
		// get the ChronoEPackage that contains the EClass-to-ID mapping
		ChronoEPackageRegistry cep = ctx.getChronoEPackage();
		// get the EClass vertex
		Vertex eClassVertex = ChronoSphereGraphFormat.getVertexForEClass(cep, graph, eObject.eClass());
		// create the edge label
		String label = ChronoSphereGraphFormat.createEClassReferenceEdgeLabel();
		// make sure that the edge exists, and there is only one such edge
		GremlinUtils.setEdgeTarget(vertex, label, eClassVertex);
	}

}
