package org.chronos.chronosphere.testutils;

import static com.google.common.base.Preconditions.*;

import java.util.List;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

public class EMFTestUtils {

	public static EClassifier getEClassifierRecursive(final EPackage rootEPackage, final String classifierName) {
		checkNotNull(rootEPackage, "Precondition violation - argument 'rootEPackage' must not be NULL!");
		checkNotNull(classifierName, "Precondition violation - argument 'classifierName' must not be NULL!");
		EClassifier eClassifier = rootEPackage.getEClassifier(classifierName);
		if (eClassifier != null) {
			return eClassifier;
		}
		for (EPackage ePackage : rootEPackage.getESubpackages()) {
			eClassifier = getEClassifierRecursive(ePackage, classifierName);
			if (eClassifier != null) {
				return eClassifier;
			}
		}
		return null;
	}

	public static EClass getEClassRecursive(final EPackage rootEPackage, final String className) {
		checkNotNull(rootEPackage, "Precondition violation - argument 'rootEPackage' must not be NULL!");
		checkNotNull(className, "Precondition violation - argument 'className' must not be NULL!");
		EClass eClass = (EClass) rootEPackage.getEClassifier(className);
		if (eClass != null) {
			return eClass;
		}
		for (EPackage ePackage : rootEPackage.getESubpackages()) {
			eClass = getEClassRecursive(ePackage, className);
			if (eClass != null) {
				return eClass;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static void addToEReference(final EObject owner, final EReference reference, final EObject target) {
		checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
		checkNotNull(reference, "Precondition violation - argument 'reference' must not be NULL!");
		checkArgument(reference.isMany(), "Precondition violation - argument 'reference' must be many-valued!");
		checkNotNull(target, "Precondition violation - argument 'target' must not be NULL!");
		Object eGet = owner.eGet(reference);
		List<EObject> list = (List<EObject>) eGet;
		list.add(target);
	}

	@SuppressWarnings("unchecked")
	public static void removeFromEReference(final EObject owner, final EReference reference, final EObject target) {
		checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
		checkNotNull(reference, "Precondition violation - argument 'reference' must not be NULL!");
		checkArgument(reference.isMany(), "Precondition violation - argument 'reference' must be many-valued!");
		checkNotNull(target, "Precondition violation - argument 'target' must not be NULL!");
		Object eGet = owner.eGet(reference);
		List<EObject> list = (List<EObject>) eGet;
		list.remove(target);
	}
}
