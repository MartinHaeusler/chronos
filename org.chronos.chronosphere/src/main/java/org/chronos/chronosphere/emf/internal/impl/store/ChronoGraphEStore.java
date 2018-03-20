package org.chronos.chronosphere.emf.internal.impl.store;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.impl.ChronoEObjectImpl;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.chronos.chronosphere.internal.ogm.api.VertexKind;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ChronoGraphEStore extends AbstractChronoEStore {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private final ChronoSphereTransactionInternal owningTransaction;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public ChronoGraphEStore(final ChronoSphereTransactionInternal owningTransaction) {
		checkNotNull(owningTransaction, "Precondition violation - argument 'owningTransaction' must not be NULL!");
		this.owningTransaction = owningTransaction;
	}

	// =====================================================================================================================
	// ESTORE API
	// =====================================================================================================================

	@Override
	public Object get(final InternalEObject object, final EStructuralFeature feature, final int index) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		if (feature.isMany()) {
			EList<Object> list = this.getListOfValuesFor(ePackage, vertex, feature);
			if (index == NO_INDEX) {
				return list;
			} else {
				return list.get(index);
			}
		} else {
			return this.getSingleValueFor(ePackage, vertex, feature);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object set(final InternalEObject object, final EStructuralFeature feature, final int index,
			final Object value) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		Object result = null;
		if (index == NO_INDEX) {
			if (feature.isMany()) {
				// multiplicity-many feature
				if (value == null) {
					if (feature instanceof EAttribute) {
						EAttribute eAttribute = (EAttribute) feature;
						ChronoSphereGraphFormat.setEAttributeValues(ePackage, vertex, eAttribute, null);
					} else if (feature instanceof EReference) {
						EReference eReference = (EReference) feature;
						ChronoSphereGraphFormat.setEReferenceTargets(ePackage, vertex, eReference, null);
					} else {
						throw unknownFeatureTypeException(feature);
					}
				} else {
					if (feature instanceof EAttribute) {
						EAttribute eAttribute = (EAttribute) feature;
						ChronoSphereGraphFormat.setEAttributeValues(ePackage, vertex, eAttribute,
								(Collection<?>) value);
					} else if (feature instanceof EReference) {
						EReference eReference = (EReference) feature;
						List<Vertex> targetVertices = Lists.newArrayList();
						Collection<? extends EObject> targetEObjects = (Collection<? extends EObject>) value;
						for (EObject targetEObject : targetEObjects) {
							targetVertices.add(this.getEObjectVertex((ChronoEObject) targetEObject));
						}
						ChronoSphereGraphFormat.setEReferenceTargets(ePackage, vertex, eReference, targetVertices);
					} else {
						throw unknownFeatureTypeException(feature);
					}
				}
			} else {
				// multiplicity-one feature
				if (value == null) {
					if (feature instanceof EAttribute) {
						EAttribute eAttribute = (EAttribute) feature;
						ChronoSphereGraphFormat.setEAttributeValue(ePackage, vertex, eAttribute, null);
					} else if (feature instanceof EReference) {
						EReference eReference = (EReference) feature;
						ChronoSphereGraphFormat.setEReferenceTarget(ePackage, vertex, eReference, null);
					} else {
						throw unknownFeatureTypeException(feature);
					}
				} else {
					if (feature instanceof EAttribute) {
						EAttribute eAttribute = (EAttribute) feature;
						ChronoSphereGraphFormat.setEAttributeValue(ePackage, vertex, eAttribute, value);
					} else if (feature instanceof EReference) {
						EReference eReference = (EReference) feature;
						Vertex targetVertex = this.getEObjectVertex((ChronoEObject) value);
						ChronoSphereGraphFormat.setEReferenceTarget(ePackage, vertex, eReference, targetVertex);
					} else {
						throw unknownFeatureTypeException(feature);
					}
				}
			}

		} else {
			// we are always dealing with a multiplicity-many feature here.
			List<Object> list = this.getListOfValuesFor(ePackage, vertex, feature);
			result = list.set(index, value);
			this.writeListOfValuesToGraph(ePackage, vertex, feature, list);
		}
		// if we are dealing with a containment reference, set the eContainer of the value to the owner of this store
		this.setEContainerReferenceIfNecessary(object, feature, value);
		return result;
	}

	@Override
	public boolean isSet(final InternalEObject object, final EStructuralFeature feature) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		// special case: a feature is always "set" if it is the container feature and eContainer != null
		if (feature instanceof EReference) {
			EReference eReference = (EReference) feature;
			if (eReference.isContainer()) {
				return this.getContainer(object) != null;
			}
		}
		if (feature.isMany()) {
			// for many-valued features, "being set" is defined as "not being empty"
			return this.getListOfValuesFor(ePackage, vertex, feature).isEmpty() == false;
		}
		if (feature instanceof EAttribute) {
			EAttribute eAttribute = (EAttribute) feature;
			return ChronoSphereGraphFormat.getEAttributeValue(ePackage, vertex, eAttribute) != null;
		} else if (feature instanceof EReference) {
			EReference eReference = (EReference) feature;
			return ChronoSphereGraphFormat.getEReferenceTarget(ePackage, vertex, eReference) != null;
		} else {
			throw unknownFeatureTypeException(feature);
		}
	}

	@Override
	public void unset(final InternalEObject object, final EStructuralFeature feature) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		if (this.isSet(object, feature) == false) {
			// no need to unset
			return;
		}
		Object oldValue = object.eGet(feature);
		this.unsetEContainerReferenceIfNecessary(eObject, feature, NO_INDEX);
		this.removeFromEOppositeIfNecessary(eObject, feature, oldValue);
		if (feature.isMany() == false) {
			if (feature instanceof EAttribute) {
				EAttribute eAttribute = (EAttribute) feature;
				ChronoSphereGraphFormat.setEAttributeValue(ePackage, vertex, eAttribute, null);
			} else if (feature instanceof EReference) {
				EReference eReference = (EReference) feature;
				ChronoSphereGraphFormat.setEReferenceTarget(ePackage, vertex, eReference, null);
			} else {
				throw unknownFeatureTypeException(feature);
			}
		} else {
			this.writeListOfValuesToGraph(ePackage, vertex, feature, null);
		}
	}

	@Override
	public boolean isEmpty(final InternalEObject object, final EStructuralFeature feature) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		return this.getListOfValuesFor(ePackage, vertex, feature).isEmpty();
	}

	@Override
	public int size(final InternalEObject object, final EStructuralFeature feature) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		return this.getListOfValuesFor(ePackage, vertex, feature).size();
	}

	@Override
	public boolean contains(final InternalEObject object, final EStructuralFeature feature, final Object value) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		return this.getListOfValuesFor(ePackage, vertex, feature).contains(value);
	}

	@Override
	public int indexOf(final InternalEObject object, final EStructuralFeature feature, final Object value) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		return this.getListOfValuesFor(ePackage, vertex, feature).indexOf(value);
	}

	@Override
	public int lastIndexOf(final InternalEObject object, final EStructuralFeature feature, final Object value) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		return this.getListOfValuesFor(ePackage, vertex, feature).lastIndexOf(value);
	}

	@Override
	public void add(final InternalEObject object, final EStructuralFeature feature, final int index,
			final Object value) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		List<Object> list = this.getListOfValuesFor(ePackage, vertex, feature);
		list.add(index, value);
		// write to graph
		this.writeListOfValuesToGraph(ePackage, vertex, feature, list);
	}

	@Override
	public Object remove(final InternalEObject object, final EStructuralFeature feature, final int index) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		// special case: if we are removing a contained EObject, we need to unset it's eContainer
		if (feature instanceof EReference && ((EReference) feature).isContainment()) {
			Object child = this.get(object, feature, index);
			this.unsetEContainerReferenceIfNecessary(eObject, feature, index);
			List<Object> values = this.getListOfValuesFor(ePackage, vertex, feature);
			values.remove(child);
			this.writeListOfValuesToGraph(ePackage, vertex, feature, values);
			return child;
		}
		List<Object> list = this.getListOfValuesFor(ePackage, vertex, feature);
		Object result = list.remove(index);
		this.writeListOfValuesToGraph(ePackage, vertex, feature, list);
		return result;
	}

	@Override
	public Object move(final InternalEObject object, final EStructuralFeature feature, final int targetIndex,
			final int sourceIndex) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		EList<Object> values = this.getListOfValuesFor(ePackage, vertex, feature);
		Object result = values.move(targetIndex, sourceIndex);
		this.writeListOfValuesToGraph(ePackage, vertex, feature, values);
		return result;
	}

	@Override
	public void clear(final InternalEObject object, final EStructuralFeature feature) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		// if the feature is not set, we don't need to do anything...
		if (this.isSet(object, feature) == false) {
			return;
		}
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		// if the feature is a containment reference, clear the eContainer of all children
		this.unsetEContainerReferenceIfNecessary(eObject, feature, NO_INDEX);
		this.writeListOfValuesToGraph(ePackage, vertex, feature, null);
	}

	@Override
	public Object[] toArray(final InternalEObject object, final EStructuralFeature feature) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		return this.getListOfValuesFor(ePackage, vertex, feature).toArray();
	}

	@Override
	@SuppressWarnings("hiding")
	public <T> T[] toArray(final InternalEObject object, final EStructuralFeature feature, final T[] array) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		return this.getListOfValuesFor(ePackage, vertex, feature).toArray(array);
	}

	@Override
	public int hashCode(final InternalEObject object, final EStructuralFeature feature) {
		this.assertTxOpen();
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		if (this.isSet(object, feature) == false) {
			return 0;
		} else {
			ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
			Vertex vertex = this.getEObjectVertex(eObject);
			ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
			if (feature.isMany()) {
				return this.getListOfValuesFor(ePackage, vertex, feature).hashCode();
			} else {
				return this.getSingleValueFor(ePackage, vertex, feature).hashCode();
			}
		}
	}

	@Override
	public void setContainer(final InternalEObject object, final InternalEObject newContainer) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		if (newContainer != null) {
			// change container
			Vertex targetVertex = this.getEObjectVertex(this.assertIsChronoEObject(newContainer));
			ChronoSphereGraphFormat.setEContainer(vertex, targetVertex);
		} else {
			// unset container
			ChronoSphereGraphFormat.setEContainer(vertex, null);
		}

	}

	@Override
	public void setContainingFeatureID(final InternalEObject object, final int newContainerFeatureID) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoSphereGraphFormat.setEContainingFeatureId(vertex, newContainerFeatureID);
	}

	@Override
	public InternalEObject getContainer(final InternalEObject object) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		if (vertex == null) {
			return null;
		}
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		Vertex eContainerVertex = ChronoSphereGraphFormat.getEContainer(vertex);
		if (eContainerVertex == null) {
			return null;
		}
		return this.createEObjectForVertex(ePackage, eContainerVertex);
	}

	@Override
	public EStructuralFeature getContainingFeature(final InternalEObject object) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		Integer eContainingFeatureID = ChronoSphereGraphFormat.getEContainingFeatureId(vertex);
		if (eContainingFeatureID == null) {
			return null;
		}
		ChronoEObjectInternal eContainer = (ChronoEObjectInternal) this.getContainer(eObject);
		// prepare a variable to hold the containing feature
		EStructuralFeature containingFeature = null;
		if (eContainingFeatureID < 0) {
			// inverse feature
			return eContainer.eClass().getEStructuralFeature(EOPPOSITE_FEATURE_BASE - eContainingFeatureID);
		} else {
			// normal feature
			// A containing feature is set on this EObject that is a NORMAL EReference. Therefore, there must
			// be an EOpposite on the EContainer that has 'containment = true'.
			containingFeature = ((EReference) object.eClass().getEStructuralFeature(eContainingFeatureID))
					.getEOpposite();
		}
		if (eContainingFeatureID != null && containingFeature == null) {
			throw new IllegalStateException("Could not resolve Feature ID '" + eContainingFeatureID
					+ "' in eContainer '" + eContainer + "' (EClass: '" + eContainer.eClass().getName() + "')!");
		}
		return containingFeature;
	}

	@Override
	public int getContainingFeatureID(final InternalEObject object) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		Integer eContainingFeatureID = ChronoSphereGraphFormat.getEContainingFeatureId(vertex);
		if (eContainingFeatureID == null) {
			return 0;
		} else {
			return eContainingFeatureID;
		}
	}

	@Override
	public EObject create(final EClass eClass) {
		this.assertTxOpen();
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		EPackage ePackage = eClass.getEPackage();
		while (ePackage.getESuperPackage() != null) {
			ePackage = ePackage.getESuperPackage();
		}
		ChronoEPackageRegistry cep = this.owningTransaction.getEPackageRegistry();
		Vertex vertex = this.getGraph().addVertex(T.id, UUID.randomUUID());
		ChronoSphereGraphFormat.setVertexKind(vertex, VertexKind.EOBJECT);
		ChronoSphereGraphFormat.setEClassForEObjectVertex(cep, vertex, eClass);
		return this.createEObjectForVertex(cep, vertex);
	}

	@Override
	public void clearEContainerAndEContainingFeatureSilent(final InternalEObject object) {
		this.assertTxOpen();
		ChronoEObjectInternal eObject = this.assertIsChronoEObject(object);
		Vertex vertex = this.getEObjectVertex(eObject);
		ChronoSphereGraphFormat.setEContainer(vertex, null);
		ChronoSphereGraphFormat.setEContainingFeatureId(vertex, null);
	}

	// =====================================================================================================================
	// MISCELLANEOUS PUBLIC API
	// =====================================================================================================================

	public void deepMerge(final Collection<ChronoEObjectInternal> mergeObjects) {
		this.deepMerge(mergeObjects, null, false, 0);
	}

	public void deepMergeIncremental(final Collection<ChronoEObjectInternal> mergeObjects,
			final ChronoSphereTransaction tx, final int batchSize) {
		checkNotNull(mergeObjects, "Precondition violation - argument 'eObject' must not be NULL!");
		checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
		checkArgument(batchSize > 0, "Precondition violation - argument 'batchSize' must be greater than zero!");
		this.deepMerge(mergeObjects, tx, true, batchSize);
	}

	public void deepDelete(final Collection<ChronoEObjectInternal> eObjectsToDelete,
			final boolean cascadeDeletionToEContents) {
		checkNotNull(eObjectsToDelete, "Precondition violation - argument 'eObjectsToDelete' must not be NULL!");
		this.deepDelete(eObjectsToDelete, null, false, 0, cascadeDeletionToEContents);
	}

	public void deepDelete(final Collection<ChronoEObjectInternal> eObjectsToDelete, final ChronoSphereTransaction tx,
			final int batchSize, final boolean cascadeDeletionToEContents) {
		this.deepDelete(eObjectsToDelete, tx, true, batchSize, cascadeDeletionToEContents);
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	private void deepMerge(final Collection<ChronoEObjectInternal> mergeObjects, final ChronoSphereTransaction tx,
			final boolean useIncrementalCommits, final int batchSize) {
		Set<ChronoEObjectInternal> objectsToMerge = Sets.newHashSet();
		for (ChronoEObjectInternal eObject : mergeObjects) {
			TreeIterator<EObject> contents = eObject.eAllContents();
			objectsToMerge.add(eObject);
			while (contents.hasNext()) {
				objectsToMerge.add((ChronoEObjectInternal) contents.next());
			}
		}
		int currentBatchSize = 0;
		// in the first iteration, create the EObject vertices in the graph and merge the EAttributes
		for (ChronoEObjectInternal currentEObject : objectsToMerge) {
			this.mergeObjectAndAttributes(currentEObject);
			currentBatchSize++;
			if (useIncrementalCommits && currentBatchSize >= batchSize) {
				tx.commitIncremental();
				currentBatchSize = 0;
			}
		}
		// having created all vertices, we can now merge the eReferences
		for (ChronoEObjectInternal currentEObject : objectsToMerge) {
			this.mergeEReferencesAndEContainer(currentEObject);
			currentBatchSize++;
			if (useIncrementalCommits && currentBatchSize >= batchSize) {
				tx.commitIncremental();
				currentBatchSize = 0;
			}
		}
		// as the last step, replace the in-memory stores with the graph store
		for (ChronoEObjectInternal currentEObject : objectsToMerge) {
			currentEObject.eSetStore(this);
			currentBatchSize++;
			if (useIncrementalCommits && currentBatchSize >= batchSize) {
				tx.commitIncremental();
				currentBatchSize = 0;
			}
		}
	}

	private void mergeObjectAndAttributes(final ChronoEObjectInternal eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		if (eObject.isAttached()) {
			// already attached to the store, nothing to merge
			return;
		}
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		// create a vertex for the eObject
		Vertex vertex = this.createVertexForEObject(eObject);
		for (EAttribute eAttribute : eObject.eClass().getEAllAttributes()) {
			if (eObject.eIsSet(eAttribute) == false) {
				// ignore eAttributes that have no value assigned in the given EObject
				continue;
			}
			Object value = eObject.eGet(eAttribute);
			if (eAttribute.isMany()) {
				ChronoSphereGraphFormat.setEAttributeValues(ePackage, vertex, eAttribute, (Collection<?>) value);
			} else {
				ChronoSphereGraphFormat.setEAttributeValue(ePackage, vertex, eAttribute, value);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void mergeEReferencesAndEContainer(final ChronoEObjectInternal eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		if (eObject.isAttached()) {
			// already attached to the store, nothing to merge
			return;
		}
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		// get the vertex for this EObject
		Vertex vertex = Iterators.getOnlyElement(this.getGraph().vertices(eObject.getId()));
		// set the eContainer (if any)
		ChronoEObjectInternal eContainer = (ChronoEObjectInternal) eObject.eContainer();
		if (eContainer != null) {
			Vertex eContainerVertex = this.getEObjectVertex(eContainer);
			ChronoSphereGraphFormat.setEContainer(vertex, eContainerVertex);
			EStructuralFeature eContainingFeature = eObject.eContainingFeature();
			if (eContainingFeature != null) {
				int containingFeatureID = -1;
				if (eObject.eClass().getFeatureID(eContainingFeature) >= 0) {
					// containing feature belongs to our class, use the ID
					containingFeatureID = eObject.eClass().getFeatureID(eContainingFeature);
				} else {
					// containing feature belongs to eContainer class
					containingFeatureID = -1 - eContainingFeature.getFeatureID();
				}
				ChronoSphereGraphFormat.setEContainingFeatureId(vertex, containingFeatureID);
			}
		}
		// map the EReferences
		for (EReference eReference : eObject.eClass().getEAllReferences()) {
			if (eObject.eIsSet(eReference) == false) {
				// reference is not set on this EObject, nothing to merge
				continue;
			}
			Object value = eObject.eGet(eReference);
			if (eReference.isMany()) {
				List<ChronoEObjectInternal> targets = (List<ChronoEObjectInternal>) value;
				List<Vertex> targetVertices = targets.stream().map(obj -> this.getEObjectVertex(obj))
						.filter(obj -> obj != null).collect(Collectors.toList());
				ChronoSphereGraphFormat.setEReferenceTargets(ePackage, vertex, eReference, targetVertices);
			} else {
				ChronoEObjectInternal target = (ChronoEObjectInternal) value;
				Vertex targetVertex = this.getEObjectVertex(target);
				if (targetVertex == null) {
					// the target EObject is not attached to the store!
					continue;
				}
				ChronoSphereGraphFormat.setEReferenceTarget(ePackage, vertex, eReference, targetVertex);
			}
		}
	}

	private Vertex createVertexForEObject(final ChronoEObjectInternal eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		ChronoEPackageRegistry ePackage = this.getEPackageRegistry();
		Vertex vertex = this.getGraph().addVertex(T.id, eObject.getId());
		ChronoSphereGraphFormat.setVertexKind(vertex, VertexKind.EOBJECT);
		ChronoSphereGraphFormat.setEClassForEObjectVertex(ePackage, vertex, eObject.eClass());
		return vertex;
	}

	private void assertTxOpen() {
		if (this.owningTransaction.isClosed()) {
			throw new IllegalStateException("ChronoSphereTransaction has already been closed!");
		}
	}

	private ChronoGraph getGraph() {
		return this.owningTransaction.getGraph();
	}

	private ChronoEObjectInternal assertIsChronoEObject(final EObject eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		checkArgument(eObject instanceof ChronoEObject,
				"Precondition violation - argument 'eObject' is no ChronoEObject! Did you use the correct EFactory in your EPackage?");
		return (ChronoEObjectInternal) eObject;
	}

	private Vertex getEObjectVertex(final ChronoEObject object) {
		String id = object.getId();
		Iterator<Vertex> iterator = this.getGraph().vertices(id);
		return Iterators.getOnlyElement(iterator, null);
	}

	private ChronoEPackageRegistry getEPackageRegistry() {
		return this.owningTransaction.getEPackageRegistry();
	}

	private ChronoEObjectInternal createEObjectForVertex(final ChronoEPackageRegistry cep, final Vertex vertex) {
//		checkNotNull(cep, "Precondition violation - argument 'cep' must not be NULL!");
//		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
//		String id = (String) vertex.id();
//		EClass eClass = ChronoSphereGraphFormat.getEClassForEObjectVertex(cep, vertex);
//		ChronoEObjectInternal eObject = new ChronoEObjectImpl(id, eClass, this);
//		return eObject;
		return (ChronoEObjectInternal)this.owningTransaction.getEObjectById((String)vertex.id());
	}

	private EList<Object> getListOfValuesFor(final ChronoEPackageRegistry cep, final Vertex vertex,
			final EStructuralFeature feature) {
		checkNotNull(cep, "Precondition violation - argument 'cep' must not be NULL!");
		checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		EList<Object> eList = new BasicEList<>();
		if (feature instanceof EAttribute) {
			EAttribute eAttribute = (EAttribute) feature;
			eList.addAll(ChronoSphereGraphFormat.getEAttributeValues(cep, vertex, eAttribute));
			return eList;
		} else if (feature instanceof EReference) {
			EReference eReference = (EReference) feature;
			List<Vertex> vertices = ChronoSphereGraphFormat.getEReferenceTargets(cep, vertex, eReference);
			for (Vertex eObjectVertex : vertices) {
				ChronoEObjectInternal eObjectForVertex = this.createEObjectForVertex(cep, eObjectVertex);
				eList.add(eObjectForVertex);
			}
			return eList;
		} else {
			throw unknownFeatureTypeException(feature);
		}
	}

	private Object getSingleValueFor(final ChronoEPackageRegistry ePackage, final Vertex vertex,
			final EStructuralFeature feature) {
		if (feature instanceof EAttribute) {
			EAttribute attribute = (EAttribute) feature;
			return ChronoSphereGraphFormat.getEAttributeValue(ePackage, vertex, attribute);
		} else if (feature instanceof EReference) {
			EReference reference = (EReference) feature;
			Vertex targetVertex = ChronoSphereGraphFormat.getEReferenceTarget(ePackage, vertex, reference);
			if (targetVertex == null) {
				return null;
			} else {
				return this.createEObjectForVertex(ePackage, targetVertex);
			}
		} else {
			throw unknownFeatureTypeException(feature);
		}
	}

	private static RuntimeException unknownFeatureTypeException(final EStructuralFeature feature) {
		String className = feature.getClass().getName();
		return new RuntimeException("Encountered unknown subclass of EStructuralFeature: '" + className + "'!");
	}

	private void writeListOfValuesToGraph(final ChronoEPackageRegistry ePackage, final Vertex vertex,
			final EStructuralFeature feature, final List<Object> list) {
		if (feature instanceof EAttribute) {
			EAttribute eAttribute = (EAttribute) feature;
			ChronoSphereGraphFormat.setEAttributeValues(ePackage, vertex, eAttribute, Lists.newArrayList(list));
		} else if (feature instanceof EReference) {
			EReference eReference = (EReference) feature;
			if (list == null || list.isEmpty()) {
				// "unset" the reference, clear it in the graph
				ChronoSphereGraphFormat.setEReferenceTargets(ePackage, vertex, eReference, null);
			} else {
				// for each target EObject, identify the corresponding vertex
				List<Vertex> targetVertices = Lists.newArrayList();
				for (Object target : list) {
					// we already know it's an EObject, because EReference targets can only be EObjects.
					EObject targetEObject = (EObject) target;
					targetVertices.add(this.getEObjectVertex((ChronoEObject) targetEObject));
				}
				ChronoSphereGraphFormat.setEReferenceTargets(ePackage, vertex, eReference, targetVertices);
			}
		} else {
			throw unknownFeatureTypeException(feature);
		}
	}

	private void deepDelete(final Collection<ChronoEObjectInternal> eObjectsToDelete, final ChronoSphereTransaction tx,
			final boolean useIncrementalCommits, final int batchSize, final boolean cascadeDeletionToEContents) {
		Set<ChronoEObjectInternal> allEObjectsToDelete = Sets.newHashSet();
		if (cascadeDeletionToEContents) {
			for (ChronoEObjectInternal eObject : eObjectsToDelete) {
				TreeIterator<EObject> contents = eObject.eAllContents();
				allEObjectsToDelete.add(eObject);
				while (contents.hasNext()) {
					allEObjectsToDelete.add((ChronoEObjectInternal) contents.next());
				}
			}
		} else {
			allEObjectsToDelete.addAll(eObjectsToDelete);
		}
		int currentBatchSize = 0;
		// in the first iteration, create the EObject vertices in the graph and merge the EAttributes
		for (ChronoEObjectInternal currentEObject : allEObjectsToDelete) {
			Vertex vertex = ChronoSphereGraphFormat.getVertexForEObject(this.getGraph(), currentEObject);
			if (vertex == null) {
				// already deleted
				continue;
			}
			vertex.remove();
			currentBatchSize++;
			if (useIncrementalCommits && currentBatchSize >= batchSize) {
				tx.commitIncremental();
				currentBatchSize = 0;
			}
		}
	}

}
