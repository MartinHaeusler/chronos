package org.chronos.chronosphere.api.query;

import java.util.Iterator;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

public interface QueryStepBuilderStarter {

	public EObjectQueryStepBuilder<EObject> startingFromAllEObjects();

	public EObjectQueryStepBuilder<EObject> startingFromInstancesOf(String eClassName);

	public EObjectQueryStepBuilder<EObject> startingFromInstancesOf(EClass eClass);

	public EObjectQueryStepBuilder<EObject> startingFromEObjectsWith(EAttribute attribute, Object value);

	public EObjectQueryStepBuilder<EObject> startingFromEObject(EObject eObject);

	public EObjectQueryStepBuilder<EObject> startingFromEObjects(Iterable<? extends EObject> eObjects);

	public EObjectQueryStepBuilder<EObject> startingFromEObjects(Iterator<? extends EObject> eObjects);

}
