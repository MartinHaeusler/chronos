package org.chronos.chronosphere.testmodels.instance;

import java.util.Set;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

public interface TestModel extends Iterable<EObject> {

	public EPackage getEPackage();

	public Set<EObject> getAllEObjects();

	public EObject getEObjectByID(String id);

}
