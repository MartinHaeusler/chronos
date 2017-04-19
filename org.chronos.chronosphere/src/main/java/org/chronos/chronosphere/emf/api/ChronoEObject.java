package org.chronos.chronosphere.emf.api;

import org.eclipse.emf.ecore.InternalEObject;

public interface ChronoEObject extends InternalEObject {

	public String getId();

	public boolean exists();

}
