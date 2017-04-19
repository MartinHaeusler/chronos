package org.chronos.chronosphere.internal.ogm.api;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

public interface ChronoEPackageRegistryInternal extends ChronoEPackageRegistry {

	public void registerEPackage(final EPackage ePackage);

	public void registerEClassID(final EClass eClass, final String chronoEClassID);

	public void registerEAttributeID(final EAttribute eAttribute, final String chronoEAttributeID);

	public void registerEReferenceID(final EReference eReference, final String chronoEReferenceID);

	public void seal();
}
