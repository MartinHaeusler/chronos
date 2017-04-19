package org.chronos.chronosphere.emf.internal.api;

import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.internal.impl.store.ChronoGraphEStore;
import org.eclipse.emf.ecore.InternalEObject;

public interface ChronoEObjectInternal extends ChronoEObject, InternalEObject {

	public ChronoEObjectLifecycle getLifecycleStatus();

	public void setLifecycleStatus(ChronoEObjectLifecycle status);

	public void unsetEContainerSilent();

	public default boolean isAttached() {
		return this.eStore() instanceof ChronoGraphEStore;
	}

}
