package org.chronos.chronosphere.testutils.factories;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.impl.EFactoryImpl;
import org.eclipse.emf.ecore.impl.EStoreEObjectImpl;

public class EStoreEFactory extends EFactoryImpl {

	@Override
	protected EObject basicCreate(final EClass eClass) {
		return new EStoreEObjectImpl(eClass, new EStoreEObjectImpl.EStoreImpl());
	}

}
