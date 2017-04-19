package org.chronos.chronosphere.emf.impl;

import static com.google.common.base.Preconditions.*;

import java.util.UUID;

import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectLifecycle;
import org.chronos.chronosphere.emf.internal.impl.store.ChronoEStore;
import org.chronos.chronosphere.emf.internal.impl.store.OwnedTransientEStore;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.impl.EStoreEObjectImpl;
import org.eclipse.emf.ecore.impl.MinimalEStoreEObjectImpl;

public class ChronoEObjectImpl extends MinimalEStoreEObjectImpl implements ChronoEObject, ChronoEObjectInternal {

	private String id;
	private ChronoEObjectLifecycle lifecycleStatus;

	private ChronoEStore eStore;

	public ChronoEObjectImpl() {
		// by default, we always assign a new ID.
		this.id = UUID.randomUUID().toString();
		// by default, we assume the TRANSIENT state (i.e. the object has never been persisted to the repository)
		this.lifecycleStatus = ChronoEObjectLifecycle.TRANSIENT;
		// since we are in transient mode, we cannot rely on the repository to manage the EStore for us, we
		// have to do it ourselves with an in-memory store.
		this.eSetStore(new OwnedTransientEStore(this));
	}

	public ChronoEObjectImpl(final String id, final EClass eClass) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		this.id = id;
		// by default, we assume the TRANSIENT state (i.e. the object has never been persisted to the repository)
		this.lifecycleStatus = ChronoEObjectLifecycle.TRANSIENT;
		// since we are in transient mode, we cannot rely on the repository to manage the EStore for us, we
		// have to do it ourselves with an in-memory store.
		this.eSetStore(new OwnedTransientEStore(this));
		this.eSetClass(eClass);
	}

	public ChronoEObjectImpl(final String id, final EClass eClass, final ChronoEStore eStore) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		checkNotNull(eStore, "Precondition violation - argument 'eStore' must not be NULL!");
		this.id = id;
		this.eSetClass(eClass);
		this.eStore = eStore;
		this.lifecycleStatus = ChronoEObjectLifecycle.PERSISTENT_CLEAN;
	}

	// =====================================================================================================================
	// ECORE IMPLEMENTATION OVERRIDES
	// =====================================================================================================================

	@Override
	protected boolean eIsCaching() {
		// don't cache on EObject level; we do this ourselves
		return false;
	}

	@Override
	public Object dynamicGet(final int dynamicFeatureID) {
		final EStructuralFeature feature = this.eDynamicFeature(dynamicFeatureID);
		if (feature.isMany()) {
			return new EStoreEObjectImpl.BasicEStoreEList<Object>(this, feature);
		} else {
			return this.eStore().get(this, feature, EStore.NO_INDEX);
		}
	}

	@Override
	public void dynamicSet(final int dynamicFeatureID, final Object value) {
		EStructuralFeature feature = this.eDynamicFeature(dynamicFeatureID);
		if (feature.isMany()) {
			this.eStore().unset(this, feature);
			@SuppressWarnings("rawtypes")
			EList collection = (EList) value;
			for (int index = 0; index < collection.size(); index++) {
				this.eStore().set(this, feature, index, value);
			}
		} else {
			this.eStore().set(this, feature, InternalEObject.EStore.NO_INDEX, value);
		}
	}

	@Override
	public void dynamicUnset(final int dynamicFeatureID) {
		EStructuralFeature feature = this.eDynamicFeature(dynamicFeatureID);
		this.eStore().unset(this, feature);
	}

	@Override
	public void eDynamicUnset(final EStructuralFeature eFeature) {
		this.eStore().unset(this, eFeature);
	}

	@Override
	protected void eDynamicUnset(final int dynamicFeatureID, final EStructuralFeature eFeature) {
		this.eStore().unset(this, eFeature);
	}

	@Override
	protected void eBasicSetContainer(final InternalEObject newContainer) {
		this.eStore.setContainer(this, newContainer);
	}

	@Override
	protected void eBasicSetContainerFeatureID(final int newContainerFeatureID) {
		this.eStore.setContainingFeatureID(this, newContainerFeatureID);
	}

	@Override
	public void unsetEContainerSilent() {
		this.eStore().clearEContainerAndEContainingFeatureSilent(this);
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public ChronoEObjectLifecycle getLifecycleStatus() {
		return this.lifecycleStatus;
	}

	@Override
	public void setLifecycleStatus(final ChronoEObjectLifecycle status) {
		checkNotNull(status, "Precondition violation - argument 'status' must not be NULL!");
		this.lifecycleStatus = status;
	}

	@Override
	public boolean exists() {
		return this.getLifecycleStatus().isRemoved() == false;
	}

	@Override
	public ChronoEStore eStore() {
		return this.eStore;
	}

	@Override
	public void eSetStore(final EStore store) {
		this.eStore = (ChronoEStore) store;
	}

	// @Override
	// public int eContainerFeatureID() {
	// return this.eStore.getContainingFeatureID(this);
	// }

	// =====================================================================================================================
	// HASH CODE & EQUALS (based on ID only)
	// =====================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.id == null ? 0 : this.id.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		ChronoEObjectImpl other = (ChronoEObjectImpl) obj;
		if (this.id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!this.id.equals(other.id)) {
			return false;
		}
		return true;
	}

	// =====================================================================================================================
	// TOSTRING
	// =====================================================================================================================

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.eClass().getName() + "@" + this.id);
		return builder.toString();
	}

}
