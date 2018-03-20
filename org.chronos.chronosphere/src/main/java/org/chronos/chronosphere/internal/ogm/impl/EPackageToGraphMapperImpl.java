package org.chronos.chronosphere.internal.ogm.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.emf.impl.ChronoEFactory;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistryInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.EPackageBundle;
import org.chronos.chronosphere.internal.ogm.api.EPackageToGraphMapper;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class EPackageToGraphMapperImpl implements EPackageToGraphMapper {

	@Override
	public void mapToGraph(final ChronoGraph graph, final EPackageBundle bundle) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(bundle, "Precondition violation - argument 'bundle' must not be NULL!");
		this.mergeEPackageBundleIntoGraph(bundle, graph);
	}

	@Override
	public ChronoEPackageRegistry readChronoEPackageRegistryFromGraph(final ChronoGraph graph) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		ChronoEPackageRegistryInternal registry = new ChronoEPackageRegistryImpl();
		Set<Vertex> bundleVertices = ChronoSphereGraphFormat.getEpackageBundleVertices(graph);
		for (Vertex bundleVertex : bundleVertices) {
			String xmi = ChronoSphereGraphFormat.getXMIContents(bundleVertex);
			List<EPackage> ePackages = EMFUtils.readEPackagesFromXMI(xmi);
			EMFUtils.flattenEPackages(ePackages)
					.forEach(ePackage -> ePackage.setEFactoryInstance(new ChronoEFactory()));
			for (EPackage ePackage : ePackages) {
				this.registerPackageContentsRecursively(graph, ePackage, registry);
			}
		}
		return registry;
	}

	@Override
	public void deleteInGraph(final ChronoGraph graph, final EPackageBundle bundle) {
		checkNotNull(bundle, "Precondition violation - argument 'bundle' must not be NULL!");
		Vertex bundleVertex = this.getCommonBundleVertex(graph, bundle);
		if (bundleVertex == null) {
			// the bundle is not part of the graph
			return;
		}
		Iterator<Vertex> ePackageVertices = bundleVertex.vertices(Direction.OUT,
				ChronoSphereGraphFormat.E_LABEL__BUNDLE_OWNED_EPACKAGE);
		while (ePackageVertices.hasNext()) {
			Vertex ePackageVertex = ePackageVertices.next();
			this.deleteEPackageVertex(ePackageVertex);
		}
		bundleVertex.remove();
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	private void mergeEPackageBundleIntoGraph(final EPackageBundle bundle, final ChronoGraph graph) {
		ChronoEPackageRegistryInternal registry = new ChronoEPackageRegistryImpl();
		// check if one of the given EPackages is already mapped
		Vertex commonBundleVertex = null;
		for (EPackage ePackage : bundle) {
			Vertex packageVertex = ChronoSphereGraphFormat.getVertexForEPackage(graph, ePackage);
			if (packageVertex != null) {
				Iterator<Vertex> bundleVertices = packageVertex.vertices(Direction.IN,
						ChronoSphereGraphFormat.E_LABEL__BUNDLE_OWNED_EPACKAGE);
				Vertex bundleVertex = Iterators.getOnlyElement(bundleVertices);
				if (commonBundleVertex != null && commonBundleVertex.equals(bundleVertex) == false) {
					throw new IllegalStateException(
							"The EPackage '" + ePackage.getName() + "' belongs to a different bundle of EPackages!");
				}
				commonBundleVertex = bundleVertex;
			}
		}
		if (commonBundleVertex != null) {
			// we have a common bundle vertex, we need to update it
			// there might be EPackages in the old bundle that are not in the new bundle anymore
			Iterator<Vertex> ePackageVertices = commonBundleVertex.vertices(Direction.OUT,
					ChronoSphereGraphFormat.E_LABEL__BUNDLE_OWNED_EPACKAGE);
			ePackageVertices.forEachRemaining(ePackageVertex -> {
				String nsURI = ChronoSphereGraphFormat.getNsURI(ePackageVertex);
				if (bundle.containsEPackageWithNsURI(nsURI) == false) {
					// delete that EPackage
					this.deleteEPackageVertex(ePackageVertex);
				}
			});
			// update the bundle XMI
			ChronoSphereGraphFormat.updateEPackageBundleVertex(commonBundleVertex, bundle);
		} else {
			// there is no bundle vertex yet, create it
			commonBundleVertex = ChronoSphereGraphFormat.createVertexForEPackageBundle(graph, bundle);
		}
		for (EPackage ePackage : bundle) {
			this.mergeEPackageIntoGraphRecursive(commonBundleVertex, ePackage, graph);
		}
		// seal the ChronoEPackage (i.e. prevent any further modification to it)
		registry.seal();
		// map the ESuperType edges
		this.mergeESuperTypeEdges(registry, graph);
	}

	private Vertex mergeEPackageIntoGraphRecursive(final Vertex bundleVertex, final EPackage ePackage,
			final ChronoGraph graph) {
		Vertex ePackageVertex = ChronoSphereGraphFormat.getVertexForEPackage(graph, ePackage);
		if (ePackageVertex == null) {
			ePackageVertex = ChronoSphereGraphFormat.createVertexForEPackage(graph, ePackage);
		}
		if (bundleVertex != null) {
			// this EPackage is a root EPackage; create an edge from the bundle to the EPackage
			GremlinUtils.ensureEdgeExists(bundleVertex, ChronoSphereGraphFormat.E_LABEL__BUNDLE_OWNED_EPACKAGE,
					ePackageVertex);
		}
		// map the EClassifiers (EClasses)
		Set<Vertex> eClassVertices = Sets.newHashSet();
		for (EClassifier classifier : ePackage.getEClassifiers()) {
			if (classifier instanceof EClass == false) {
				// we are not interested in EDataTypes here, skip them
				continue;
			}
			EClass eClass = (EClass) classifier;
			Vertex eClassVertex = this.mergeEClassIntoGraph(ePackageVertex, eClass);
			eClassVertices.add(eClassVertex);
		}
		// remove all EClass vertices that we did not visit (they were removed in the EPackage)
		Set<Vertex> existingEClassVertices = Sets.newHashSet(
				ePackageVertex.vertices(Direction.OUT, ChronoSphereGraphFormat.E_LABEL__EPACKAGE_OWNED_CLASSIFIERS));
		existingEClassVertices.removeAll(eClassVertices);
		existingEClassVertices.forEach(eClassVertex -> this.deleteEClassVertex(eClassVertex));

		// map the child EPackages recursively
		Set<Vertex> childEPackageVertices = Sets.newHashSet();
		for (EPackage childPackage : ePackage.getESubpackages()) {
			Vertex childPackageVertex = this.mergeEPackageIntoGraphRecursive(bundleVertex, childPackage, graph);
			childEPackageVertices.add(childPackageVertex);
		}
		Set<Vertex> allSubPackageVertices = Sets
				.newHashSet(ePackageVertex.vertices(Direction.OUT, ChronoSphereGraphFormat.E_LABEL__ESUBPACKAGE));
		allSubPackageVertices.removeAll(childEPackageVertices);
		// all other subpackages have been removed
		for (Vertex subPackageVertex : allSubPackageVertices) {
			this.deleteEPackageVertex(subPackageVertex);
		}
		return ePackageVertex;
	}

	private Vertex mergeEClassIntoGraph(final Vertex ePackageVertex, final EClass eClass) {
		ChronoGraph graph = (ChronoGraph) ePackageVertex.graph();
		// try to find the EClass vertex in the graph
		Vertex eClassVertex = ChronoSphereGraphFormat.getVertexForEClassRaw(graph, eClass);
		if (eClassVertex == null) {
			// eClass has not yet been mapped; create the vertex and an ID for it
			String id = UUID.randomUUID().toString();
			eClassVertex = graph.addVertex(T.id, id);
			ChronoSphereGraphFormat.setVertexKind(eClassVertex, VertexKind.ECLASS);
			ePackageVertex.addEdge(ChronoSphereGraphFormat.E_LABEL__EPACKAGE_OWNED_CLASSIFIERS, eClassVertex);
			ChronoSphereGraphFormat.setVertexName(eClassVertex, eClass.getName());
		} else {
			// eClass has already been mapped; nothing to do
		}

		// in order to remove vertices for EReferences and EAttributes in the graph that do not exist anymore in the
		// EPackage, remember which vertices we visited
		Set<Vertex> eAttributeVertices = Sets.newHashSet();
		Set<Vertex> eReferenceVertices = Sets.newHashSet();

		// map the EAttributes of that EClass
		for (EAttribute eAttribute : eClass.getEAttributes()) {
			// try to find the EAttribute vertex in the graph
			Vertex eAttributeVertex = ChronoSphereGraphFormat.getVertexForEAttributeRaw(eClassVertex, eAttribute);
			// if no vertex exists, create it
			if (eAttributeVertex == null) {
				String id = UUID.randomUUID().toString();
				eAttributeVertex = graph.addVertex(T.id, id);
				ChronoSphereGraphFormat.setVertexKind(eAttributeVertex, VertexKind.EATTRIBUTE);
				eClassVertex.addEdge(ChronoSphereGraphFormat.E_LABEL__ECLASS_OWNED_EATTRIBUTE, eAttributeVertex);
				ChronoSphereGraphFormat.setVertexName(eAttributeVertex, eAttribute.getName());
			}
			// remember that we visited this vertex
			eAttributeVertices.add(eAttributeVertex);
		}

		// map the EReferences of that EClass
		for (EReference eReference : eClass.getEReferences()) {
			// try to find the EReference vertex in the graph
			Vertex eReferenceVertex = ChronoSphereGraphFormat.getVertexForEReferenceRaw(eClassVertex, eReference);
			// if no vertex exists, create it
			if (eReferenceVertex == null) {
				String id = UUID.randomUUID().toString();
				eReferenceVertex = graph.addVertex(T.id, id);
				ChronoSphereGraphFormat.setVertexKind(eReferenceVertex, VertexKind.EREFERENCE);
				eClassVertex.addEdge(ChronoSphereGraphFormat.E_LABEL__ECLASS_OWNED_EREFERENCE, eReferenceVertex);
				ChronoSphereGraphFormat.setVertexName(eReferenceVertex, eReference.getName());
			}
			// remember that we visited this vertex
			eReferenceVertices.add(eReferenceVertex);
		}

		// remove all EAttribute vertices that do not exist anymore in the graph
		Set<Vertex> existingEAttributeVertices = Sets.newHashSet(
				eClassVertex.vertices(Direction.OUT, ChronoSphereGraphFormat.E_LABEL__ECLASS_OWNED_EATTRIBUTE));
		existingEAttributeVertices.removeAll(eAttributeVertices);
		// all remaining eAttribute vertices represent removed eAttributes
		for (Vertex eAttributeVertex : existingEAttributeVertices) {
			eAttributeVertex.remove();
		}

		// remove all EReference vertices that do not exist anymore in the graph
		Set<Vertex> existingEReferenceVertices = Sets.newHashSet(
				eClassVertex.vertices(Direction.OUT, ChronoSphereGraphFormat.E_LABEL__ECLASS_OWNED_EREFERENCE));
		existingEReferenceVertices.removeAll(eReferenceVertices);
		// all remaining eAttribute vertices represent removed eAttributes
		for (Vertex eReferenceVertex : existingEReferenceVertices) {
			eReferenceVertex.remove();
		}

		return eClassVertex;
	}

	private void mergeESuperTypeEdges(final ChronoEPackageRegistry chronoEPackage, final ChronoGraph graph) {
		checkNotNull(chronoEPackage, "Precondition violation - argument 'chronoEPackage' must not be NULL!");
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		for (EClass eClass : chronoEPackage.getEClasses()) {
			Vertex eClassVertex = ChronoSphereGraphFormat.getVertexForEClass(chronoEPackage, graph, eClass);
			List<EClass> superTypes = eClass.getESuperTypes();
			Set<Vertex> superTypeVertices = Sets.newHashSet();
			for (EClass superType : superTypes) {
				Vertex superTypeVertex = ChronoSphereGraphFormat.getVertexForEClass(chronoEPackage, graph, superType);
				if (superTypeVertex == null) {
					throw new IllegalStateException("Could not find vertex for supertype '" + superType.getName()
							+ "' of EClass '" + eClass.getName() + "'!");
				}
				superTypeVertices.add(superTypeVertex);
			}
			GremlinUtils.setEdgeTargets(eClassVertex, ChronoSphereGraphFormat.E_LABEL__ECLASS_ESUPERTYPE,
					superTypeVertices);
		}
	}

	private void deleteEPackageVertex(final Vertex packageVertex) {
		Set<Vertex> verticesToRemove = this.deleteEPackageVertexRecursively(packageVertex);
		for (Vertex vertex : verticesToRemove) {
			vertex.remove();
		}
	}

	private Set<Vertex> deleteEPackageVertexRecursively(final Vertex packageVertex) {
		Set<Vertex> verticesToRemove = Sets.newHashSet();
		verticesToRemove.add(packageVertex);
		// traverse the eClasses
		Set<Vertex> eClassVertices = Sets.newHashSet(
				packageVertex.vertices(Direction.OUT, ChronoSphereGraphFormat.E_LABEL__EPACKAGE_OWNED_CLASSIFIERS));
		verticesToRemove.addAll(eClassVertices);
		// traverse the eAttributes, eReferences and instance EObjects
		for (Vertex eClassVertex : eClassVertices) {
			verticesToRemove.addAll(this.deleteEClassVertexRecursively(eClassVertex));
		}
		// do the same for the subpackages
		Set<Vertex> subPackageVertices = Sets
				.newHashSet(packageVertex.vertices(Direction.OUT, ChronoSphereGraphFormat.E_LABEL__ESUBPACKAGE));
		for (Vertex subPackageVertex : subPackageVertices) {
			verticesToRemove.addAll(this.deleteEPackageVertexRecursively(subPackageVertex));
		}
		return verticesToRemove;
	}

	private void deleteEClassVertex(final Vertex vertex) {
		this.deleteEClassVertexRecursively(vertex).forEach(v -> v.remove());
	}

	private Set<Vertex> deleteEClassVertexRecursively(final Vertex eClassVertex) {
		Set<Vertex> verticesToRemove = Sets.newHashSet();
		verticesToRemove.add(eClassVertex);
		// eAttributes
		Iterator<Vertex> eAttributeVertices = eClassVertex.vertices(Direction.OUT,
				ChronoSphereGraphFormat.E_LABEL__ECLASS_OWNED_EATTRIBUTE);
		eAttributeVertices.forEachRemaining(v -> verticesToRemove.add(v));
		// eReferences
		Iterator<Vertex> eReferenceVertices = eClassVertex.vertices(Direction.OUT,
				ChronoSphereGraphFormat.E_LABEL__ECLASS_OWNED_EREFERENCE);
		eReferenceVertices.forEachRemaining(v -> verticesToRemove.add(v));
		// instance EObjects
		Iterator<Vertex> eObjectVertices = eClassVertex.graph().traversal()
				// start from all vertices
				.V()
				// restrict to EObjects only
				.has(ChronoSphereGraphFormat.V_PROP__KIND, VertexKind.EOBJECT.toString())
				// restrict to EObjects which have the EClass which we want to delete
				.has(ChronoSphereGraphFormat.V_PROP__ECLASS_ID, eClassVertex.id());
		eObjectVertices.forEachRemaining(v -> verticesToRemove.add(v));
		return verticesToRemove;
	}

	private void registerPackageContentsRecursively(final ChronoGraph graph, final EPackage ePackage,
			final ChronoEPackageRegistryInternal chronoEPackage) {
		checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		checkNotNull(chronoEPackage, "Precondition violation - argument 'chronoEPackage' must not be NULL!");
		chronoEPackage.registerEPackage(ePackage);
		Vertex ePackageVertex = ChronoSphereGraphFormat.getVertexForEPackage(graph, ePackage);
		if (ePackageVertex == null) {
			throw new IllegalArgumentException("Could not find EPackage '" + ePackage.getName() + "' (NS URI: '"
					+ ePackage.getNsURI() + "') in data store! Was it registered before?");
		}
		for (EClassifier eClassifier : ePackage.getEClassifiers()) {
			if (eClassifier instanceof EClass == false) {
				// we are not interested in EDataTypes here; skip them
				continue;
			}
			EClass eClass = (EClass) eClassifier;
			Vertex eClassVertex = ChronoSphereGraphFormat.getVertexForEClassRaw(graph, eClass);
			if (eClassVertex == null) {
				throw new IllegalArgumentException("Could not find EClass '" + eClass.getName()
						+ "' in data store! Did the EPackage contents change?");
			}
			chronoEPackage.registerEClassID(eClass, (String) eClassVertex.id());
			for (EAttribute eAttribute : eClass.getEAttributes()) {
				Vertex eAttributeVertex = ChronoSphereGraphFormat.getVertexForEAttributeRaw(eClassVertex, eAttribute);
				if (eAttributeVertex == null) {
					throw new IllegalArgumentException("Could not find EAttribute '" + eClass.getName() + "#"
							+ eAttribute.getName() + "' in data store! Did the EPackage contents change?");
				}
				chronoEPackage.registerEAttributeID(eAttribute, (String) eAttributeVertex.id());
			}
			for (EReference eReference : eClass.getEReferences()) {
				Vertex eReferenceVertex = ChronoSphereGraphFormat.getVertexForEReferenceRaw(eClassVertex, eReference);
				if (eReferenceVertex == null) {
					throw new IllegalArgumentException("Could not find EReference '" + eClass.getName() + "#"
							+ eReference.getName() + "' in data store! Did the EPackage contents change?");
				}
				chronoEPackage.registerEReferenceID(eReference, (String) eReferenceVertex.id());
			}
		}
		for (EPackage subPackage : ePackage.getESubpackages()) {
			this.registerPackageContentsRecursively(graph, subPackage, chronoEPackage);
		}
	}

	private Vertex getCommonBundleVertex(final ChronoGraph graph, final EPackageBundle bundle) {
		Vertex commonBundleVertex = null;
		for (EPackage ePackage : bundle) {
			Vertex packageVertex = ChronoSphereGraphFormat.getVertexForEPackage(graph, ePackage);
			if (packageVertex != null) {
				Iterator<Vertex> bundleVertices = packageVertex.vertices(Direction.IN,
						ChronoSphereGraphFormat.E_LABEL__BUNDLE_OWNED_EPACKAGE);
				Vertex bundleVertex = Iterators.getOnlyElement(bundleVertices);
				if (commonBundleVertex != null && commonBundleVertex.equals(bundleVertex) == false) {
					throw new IllegalStateException(
							"The EPackage '" + ePackage.getName() + "' belongs to a different bundle of EPackages!");
				}
				commonBundleVertex = bundleVertex;
			}
		}
		return commonBundleVertex;
	}

}
