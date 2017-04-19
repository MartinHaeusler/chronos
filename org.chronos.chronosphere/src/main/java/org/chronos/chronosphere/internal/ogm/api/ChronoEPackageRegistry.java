package org.chronos.chronosphere.internal.ogm.api;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

public interface ChronoEPackageRegistry {

	public boolean existsEPackage(String namespaceURI);

	public EPackage getEPackage(String namespaceURI);

	public Set<EClass> getEClasses();

	public EClass getEClassByID(String chronoEClassID);

	public String getEClassID(EClass eClass);

	public EAttribute getEAttributeByID(String chronoEAttributeID);

	public String getEAttributeID(EAttribute eAttribute);

	public EReference getEReferenceByID(String chronoEReferenceID);

	public String getEReferenceID(EReference eReference);

	public Set<EPackage> getEPackages();

	@SuppressWarnings("unchecked")
	public default <T extends EStructuralFeature> T getRegisteredEStructuralFeature(final T eStructuralFeature) {
		checkNotNull(eStructuralFeature, "Precondition violation - argument 'eStructuralFeature' must not be NULL!");
		if (eStructuralFeature instanceof EAttribute) {
			EAttribute eAttribute = (EAttribute) eStructuralFeature;
			String id = this.getEAttributeID(eAttribute);
			if (id == null) {
				return null;
			}
			return (T) this.getEAttributeByID(id);
		} else if (eStructuralFeature instanceof EReference) {
			EReference eReference = (EReference) eStructuralFeature;
			String id = this.getEReferenceID(eReference);
			if (id == null) {
				return null;
			}
			return (T) this.getEReferenceByID(id);
		} else {
			throw new IllegalArgumentException(
					"Unknown subclass of EStructuralFeature: '" + eStructuralFeature.getClass().getName() + "'!");
		}
	}

}
