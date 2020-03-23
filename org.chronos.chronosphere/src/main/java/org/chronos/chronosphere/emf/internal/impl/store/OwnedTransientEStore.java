package org.chronos.chronosphere.emf.internal.impl.store;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.Map;

import org.chronos.chronosphere.emf.impl.ChronoEObjectImpl;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import com.google.common.collect.Maps;

public class OwnedTransientEStore extends AbstractChronoEStore {

	private ChronoEObjectInternal owner;

	private final Map<EStructuralFeature, Object> contents = Maps.newHashMap();
	private ChronoEObjectInternal eContainer;
	private Integer eContainingFeatureID;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	public OwnedTransientEStore(final ChronoEObjectInternal owner) {
		checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
		this.owner = owner;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public Object get(final InternalEObject object, final EStructuralFeature feature, final int index) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		if (feature.isMany()) {
			EList<Object> list = this.getListOfValuesFor(feature);
			if (index == NO_INDEX) {
				return list;
			} else {
				return list.get(index);
			}
		} else {
			return this.contents.get(feature);
		}
	}

	@Override
	public Object set(final InternalEObject object, final EStructuralFeature feature, final int index,
			final Object value) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		Object result = null;
		if (index == NO_INDEX) {
			if (value == null) {
				result = this.contents.remove(feature);
			} else {
				result = this.contents.put(feature, value);
			}
		} else {
			List<Object> list = this.getListOfValuesFor(feature);
			result = list.set(index, value);
		}
		// if we are dealing with a containment reference, set the eContainer of the value to the owner of this store
		this.setEContainerReferenceIfNecessary(object, feature, value);
		return result;
	}

	@Override
	public boolean isSet(final InternalEObject object, final EStructuralFeature feature) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		// special case for container features
		if (feature instanceof EReference && ((EReference)feature).isContainer()) {
			if (this.getContainer(object) == null) {
				return false;
			}
			final EStructuralFeature containingFeature = this.getContainingFeature(object);
			if (containingFeature == null) {
				return false;
			}
			if (containingFeature instanceof EReference) {
				final EReference containerReference = ((EReference) containingFeature).getEOpposite();
				return feature.equals(containerReference);
			}
		}
		if (feature.isMany()) {
			// for many-valued features, "being set" is defined as "not being empty"
			return this.getListOfValuesFor(feature).isEmpty() == false;
		}
		return this.contents.containsKey(feature);
	}

	@Override
	public void unset(final InternalEObject object, final EStructuralFeature feature) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		if (this.isSet(object, feature) == false) {
			// no need to unset
			return;
		}
		Object oldValue = object.eGet(feature);
		this.unsetEContainerReferenceIfNecessary(this.owner, feature, NO_INDEX);
		this.removeFromEOppositeIfNecessary(this.owner, feature, oldValue);
		this.contents.remove(feature);
	}

	@Override
	public boolean isEmpty(final InternalEObject object, final EStructuralFeature feature) {
		this.assertIsOwner(object);
		return this.getListOfValuesFor(feature).isEmpty();
	}

	@Override
	public int size(final InternalEObject object, final EStructuralFeature feature) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		return this.getListOfValuesFor(feature).size();
	}

	@Override
	public boolean contains(final InternalEObject object, final EStructuralFeature feature, final Object value) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		return this.getListOfValuesFor(feature).contains(value);
	}

	@Override
	public int indexOf(final InternalEObject object, final EStructuralFeature feature, final Object value) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		return this.getListOfValuesFor(feature).indexOf(value);
	}

	@Override
	public int lastIndexOf(final InternalEObject object, final EStructuralFeature feature, final Object value) {
		this.assertIsOwner(object);
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		return this.getListOfValuesFor(feature).lastIndexOf(value);
	}

	@Override
	public void add(final InternalEObject object, final EStructuralFeature feature, final int index,
			final Object value) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		this.getListOfValuesFor(feature).add(index, value);
		// DON'T do the following! It's already managed by Ecore
		// this.setEContainerReferenceIfNecessary(object, feature, value);
	}

	@Override
	public Object remove(final InternalEObject object, final EStructuralFeature feature, final int index) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		// special case: if we are removing a contained EObject, we need to unset it's eContainer
		if (feature instanceof EReference && ((EReference) feature).isContainment()) {
			Object child = this.get(object, feature, index);
			this.unsetEContainerReferenceIfNecessary(this.owner, feature, index);
			this.getListOfValuesFor(feature).remove(child);
			return child;
		}
		return this.getListOfValuesFor(feature).remove(index);
	}

	@Override
	public Object move(final InternalEObject object, final EStructuralFeature feature, final int targetIndex,
			final int sourceIndex) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		return this.getListOfValuesFor(feature).move(targetIndex, sourceIndex);
	}

	@Override
	public void clear(final InternalEObject object, final EStructuralFeature feature) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		// if the feature is not set, we don't need to do anything...
		if (this.isSet(object, feature) == false) {
			return;
		}
		// if the feature is a containment reference, clear the eContainer of all children
		this.unsetEContainerReferenceIfNecessary(this.owner, feature, NO_INDEX);
		// according to standard implementation "EStoreImpl" this should NOT be "getListOfValuesFor(feature).clear()"
		this.contents.remove(feature);
	}

	@Override
	public Object[] toArray(final InternalEObject object, final EStructuralFeature feature) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		return this.getListOfValuesFor(feature).toArray();
	}

	@Override
	public <T> T[] toArray(final InternalEObject object, final EStructuralFeature feature, final T[] array) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		return this.getListOfValuesFor(feature).toArray(array);
	}

	@Override
	public int hashCode(final InternalEObject object, final EStructuralFeature feature) {
		this.assertIsOwner(object);
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		if (this.isSet(object, feature) == false) {
			return 0;
		} else {
			if (feature.isMany()) {
				return this.getListOfValuesFor(feature).hashCode();
			} else {
				return this.contents.get(feature).hashCode();
			}
		}
	}

	@Override
	public void setContainer(final InternalEObject object, final InternalEObject newContainer) {
		this.assertIsOwner(object);
		this.eContainer = (ChronoEObjectInternal) newContainer;
	}

	@Override
	public void setContainingFeatureID(final InternalEObject object, final int newContainerFeatureID) {
		this.assertIsOwner(object);
		this.eContainingFeatureID = newContainerFeatureID;
	}

	@Override
	public InternalEObject getContainer(final InternalEObject object) {
		this.assertIsOwner(object);
		return this.eContainer;
	}

	@Override
	public EStructuralFeature getContainingFeature(final InternalEObject object) {
		this.assertIsOwner(object);
		if (this.eContainingFeatureID == null) {
			return null;
		}
		// prepare a variable to hold the containing feature
		EStructuralFeature containingFeature = null;
		if (this.eContainingFeatureID <= EOPPOSITE_FEATURE_BASE) {
			// inverse feature
			return this.eContainer.eClass().getEStructuralFeature(EOPPOSITE_FEATURE_BASE - this.eContainingFeatureID);
		} else {
			// normal feature
			// A containing feature is set on this EObject that is a NORMAL EReference. Therefore, there must
			// be an EOpposite on the EContainer that has 'containment = true'.
			containingFeature = ((EReference) object.eClass().getEStructuralFeature(this.eContainingFeatureID))
					.getEOpposite();
		}
		if (this.eContainingFeatureID != null && containingFeature == null) {
			throw new IllegalStateException(
					"Could not resolve Feature ID '" + this.eContainingFeatureID + "' in eContainer '" + this.eContainer
							+ "' (EClass: '" + this.eContainer.eClass().getName() + "')!");
		}
		return containingFeature;
	}

	@Override
	public int getContainingFeatureID(final InternalEObject eObject) {
		this.assertIsOwner(eObject);
		if (this.eContainingFeatureID == null) {
			return 0;
		}
		return this.eContainingFeatureID;
	}

	@Override
	public EObject create(final EClass eClass) {
		ChronoEObjectImpl eObject = new ChronoEObjectImpl();
		eObject.eSetClass(eClass);
		return eObject;
	}

	@Override
	public void clearEContainerAndEContainingFeatureSilent(final InternalEObject eObject) {
		this.assertIsOwner(eObject);
		this.eContainer = null;
		this.eContainingFeatureID = null;
	}

	// =====================================================================================================================
	// MISCELLANEOUS API
	// =====================================================================================================================

	public ChronoEObjectInternal getOwner() {
		return this.owner;
	}

	// =================================================================================================================
	// INTERNAL HELPER METHODS
	// =================================================================================================================

	@SuppressWarnings("unchecked")
	private EList<Object> getListOfValuesFor(final EStructuralFeature feature) {
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		checkArgument(feature.isMany(),
				"Precondition violation - argument 'feature' is multiplicity-one, can't get list value!");
		EList<Object> result = (EList<Object>) this.contents.get(feature);
		if (result == null) {
			result = new BasicEList<Object>();
			this.contents.put(feature, result);
		}
		return result;
	}

	private void assertIsOwner(final EObject eObject) {
		if (eObject == null) {
			throw new NullPointerException("OwnedTransientEStore can not work with EObject NULL!");
		}
		if (this.owner != eObject) {
			throw new IllegalArgumentException("OwnedTransientEStore can only work with the owning EObject!");
		}
	}
}
