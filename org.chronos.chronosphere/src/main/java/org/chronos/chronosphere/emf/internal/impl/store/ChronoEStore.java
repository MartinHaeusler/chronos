package org.chronos.chronosphere.emf.internal.impl.store;

import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.InternalEObject.EStore;

public interface ChronoEStore extends EStore {

	/**
	 * The base value for negative, i.e., opposite-end, eContainerFeatureID values.
	 */
	static final int EOPPOSITE_FEATURE_BASE = -1;

	public void setContainer(InternalEObject object, InternalEObject newContainer);

	public void setContainingFeatureID(InternalEObject object, int newContainerFeatureID);

	public int getContainingFeatureID(InternalEObject object);

	public void clearEContainerAndEContainingFeatureSilent(final InternalEObject eObject);

}
