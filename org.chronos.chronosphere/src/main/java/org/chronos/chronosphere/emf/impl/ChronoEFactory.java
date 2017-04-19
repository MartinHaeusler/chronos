package org.chronos.chronosphere.emf.impl;

import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EFactory;
import org.eclipse.emf.ecore.impl.EFactoryImpl;

public class ChronoEFactory extends EFactoryImpl implements EFactory {

	@Override
	protected ChronoEObject basicCreate(final EClass eClass) {
		ChronoEObjectImpl eObject = new ChronoEObjectImpl();
		eObject.eSetClass(eClass);
		return eObject;
	}

}
