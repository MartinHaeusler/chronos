package org.chronos.chronosphere.emf.internal.impl.store;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.List;

import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import com.google.common.collect.Lists;

public abstract class AbstractChronoEStore implements ChronoEStore {

	protected int preventCascadeUnsetContainer = 0;
	protected int preventCascadeRemoveFromOpposite = 0;

	protected void setEContainerReferenceIfNecessary(final InternalEObject object, final EStructuralFeature feature,
			final Object value) {
		// if we are dealing with a containment reference, set the eContainer of the value to the owner of this store
		if (feature instanceof EReference) {
			EReference eReference = (EReference) feature;
			if (eReference.isContainment() && value instanceof EObject) {
				// set the EContainer of the target object to the owner of this EStore
				EObject child = (EObject) value;
				if (child instanceof ChronoEObject == false) {
					throw new IllegalArgumentException(
							"Cannot form containment hierarchies between ChronoEObjects and other EObjects!");
				}
				ChronoEObject chronoChild = (ChronoEObject) child;
				int containingFeatureID = 0;
				if (eReference.getEOpposite() != null) {
					// we have an eOpposite, set that as containing feature
					containingFeatureID = eReference.getEOpposite().getFeatureID();
				} else {
					// invert the feature ID (-1 - ID) to indicate that it is a feature of the parent,
					// because we have no EOpposite
					containingFeatureID = -1 - feature.getFeatureID();
				}
				chronoChild.eBasicSetContainer(object, containingFeatureID, null);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected void unsetEContainerReferenceIfNecessary(final InternalEObject object, final EStructuralFeature feature,
			final int index) {
		if (feature instanceof EReference == false) {
			// not a reference
			return;
		}
		EReference eReference = (EReference) feature;
		if (eReference.isContainment() == false && eReference.isContainer() == false) {
			// not a containment -> no need to update any containment references
			return;
		}
		if (this.isSet(object, feature) == false) {
			// no values set -> no containments to update
			return;
		}
		if (this.preventCascadeUnsetContainer > 0) {
			// already cascading
			return;
		}
		this.preventCascadeUnsetContainer++;
		try {
			if (eReference.isContainment()) {
				// we are removing a child from our containment reference
				if (eReference.isMany()) {
					List<Object> values = Lists.newArrayList((EList<Object>) this.get(object, eReference, NO_INDEX));
					if (index == NO_INDEX) {
						// clear all children
						for (Object value : values) {
							ChronoEObjectInternal childEObject = (ChronoEObjectInternal) value;
							childEObject.unsetEContainerSilent();
						}
						return;
					} else {
						// clear a single child
						ChronoEObjectInternal childEObject = (ChronoEObjectInternal) values.get(index);
						childEObject.unsetEContainerSilent();
						return;
					}
				} else {
					// clear a single child
					ChronoEObjectInternal childEObject = (ChronoEObjectInternal) this.get(object, eReference, NO_INDEX);
					childEObject.unsetEContainerSilent();
					return;
				}
			} else if (eReference.isContainer()) {
				// we are the child and are removing ourselves from the parent
				// get the parent object and the containing feature
				EObject eContainer = this.getContainer(object);
				EStructuralFeature containingFeature = this.getContainingFeature(object);
				if (containingFeature.isMany()) {
					List<EObject> children = (List<EObject>) eContainer.eGet(containingFeature);
					children.remove(object);
					if (children.contains(object)) {
						throw new RuntimeException("Children still contains the removed object!");
					}
				} else {
					eContainer.eSet(containingFeature, null);
				}
				// we are no longer part of our container
				this.clearEContainerAndEContainingFeatureSilent(object);
			}
		} finally {
			this.preventCascadeUnsetContainer--;
		}
	}

	@SuppressWarnings("unchecked")
	protected void removeFromEOppositeIfNecessary(final ChronoEObject object, final EStructuralFeature feature,
			final Object otherEnd) {
		checkNotNull(object, "Precondition violation - argument 'object' must not be NULL!");
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		checkNotNull(otherEnd, "Precondition violation - argument 'otherEnd' must not be NULL!");
		if (feature instanceof EReference == false) {
			// no EReference -> can't have an EOpposite
			return;
		}
		EReference reference = (EReference) feature;
		if (reference.getEOpposite() == null) {
			// no opposite available
			return;
		}
		if (this.preventCascadeRemoveFromOpposite > 0) {
			// already cascading
			return;
		}
		this.preventCascadeRemoveFromOpposite++;
		try {
			EReference oppositeRef = reference.getEOpposite();
			if (reference.isMany()) {
				// remove from all opposite ends
				Collection<EObject> otherEndEObjects = Lists.newArrayList((Collection<EObject>) otherEnd);
				for (EObject otherEndEObject : otherEndEObjects) {
					if (oppositeRef.isMany()) {
						((List<EObject>) otherEndEObject.eGet(oppositeRef)).remove(object);
					} else {
						otherEndEObject.eSet(oppositeRef, null);
					}
				}
			} else {
				// remove from single opposite end
				EObject otherEndEObject = (EObject) otherEnd;
				if (oppositeRef.isMany()) {
					((List<EObject>) otherEndEObject.eGet(oppositeRef)).remove(object);
				} else {
					otherEndEObject.eSet(oppositeRef, null);
				}
			}
		} finally {
			this.preventCascadeRemoveFromOpposite--;
		}
	}

}
