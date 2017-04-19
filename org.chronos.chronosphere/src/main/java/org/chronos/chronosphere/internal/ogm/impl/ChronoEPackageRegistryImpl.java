package org.chronos.chronosphere.internal.ogm.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;
import java.util.Stack;

import org.chronos.chronosphere.emf.impl.ChronoEFactory;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistryInternal;
import org.chronos.common.logging.ChronoLogger;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;

public class ChronoEPackageRegistryImpl implements ChronoEPackageRegistryInternal {

	private final BiMap<String, EPackage> nsURItoEPackage = HashBiMap.create();
	private final BiMap<EClass, String> eClassToID = HashBiMap.create();
	private final BiMap<EAttribute, String> eAttributeToID = HashBiMap.create();
	private final BiMap<EReference, String> eReferenceToID = HashBiMap.create();

	private boolean isSealed;

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public boolean existsEPackage(final String namespaceURI) {
		checkNotNull(namespaceURI, "Precondition violation - argument 'namespaceURI' must not be NULL!");
		return this.getEPackage(namespaceURI) != null;
	}

	@Override
	public EPackage getEPackage(final String namespaceURI) {
		checkNotNull(namespaceURI, "Precondition violation - argument 'namespaceURI' must not be NULL!");
		return this.nsURItoEPackage.get(namespaceURI);
	}

	@Override
	public EClass getEClassByID(final String chronoEClassID) {
		checkNotNull(chronoEClassID, "Precondition violation - argument 'chronoEClassID' must not be NULL!");
		return this.eClassToID.inverse().get(chronoEClassID);
	}

	@Override
	public String getEClassID(final EClass eClass) {
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		String eClassID = this.eClassToID.get(eClass);
		if (eClassID != null) {
			// we found the EClass ID via '==' comparison
			return eClassID;
		}
		// we did not find the eClass instance - try to find it via owning EPackage NS URI
		EClass registeredClass = this.getRegisteredEClassforEClassViaEPackage(eClass);
		if (registeredClass != null) {
			return this.eClassToID.get(registeredClass);
		} else {
			// the ePackage must have changed in structure, can't find the class
			return null;
		}
	}

	@Override
	public Set<EClass> getEClasses() {
		return Collections.unmodifiableSet(this.eClassToID.keySet());
	}

	@Override
	public EAttribute getEAttributeByID(final String chronoEAttributeID) {
		checkNotNull(chronoEAttributeID, "Precondition violation - argument 'chronoEAttributeID' must not be NULL!");
		return this.eAttributeToID.inverse().get(chronoEAttributeID);
	}

	@Override
	public String getEAttributeID(final EAttribute eAttribute) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		String eAttributeID = this.eAttributeToID.get(eAttribute);
		if (eAttributeID != null) {
			// found the EAttribute ID via '==' comparison
			return eAttributeID;
		}
		// we did not find the EAttribute instance - try to find it via owning EClass
		EClass eClass = this.getRegisteredEClassforEClassViaEPackage(eAttribute.getEContainingClass());
		if (eClass == null) {
			// we don't know the owning EClass, maybe the EPackage was not registered?
			return null;
		}
		EStructuralFeature feature = eClass.getEStructuralFeature(eAttribute.getName());
		if (feature == null) {
			// in our version of the EClass, no such feature exists...
			return null;
		}
		if (feature instanceof EAttribute == false) {
			// in our version of the eClass, it's an EReference...
			return null;
		}
		EAttribute registeredEAttribute = (EAttribute) feature;
		return this.eAttributeToID.get(registeredEAttribute);
	}

	@Override
	public EReference getEReferenceByID(final String chronoEReferenceID) {
		checkNotNull(chronoEReferenceID, "Precondition violation - argument 'chronoEReferenceID' must not be NULL!");
		return this.eReferenceToID.inverse().get(chronoEReferenceID);
	}

	@Override
	public String getEReferenceID(final EReference eReference) {
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		String eReferenceID = this.eReferenceToID.get(eReference);
		if (eReferenceID != null) {
			// found the EReference ID via '==' comparison
			return eReferenceID;
		}
		// we did not find the EReference instance - try to find it via owning EClass
		EClass eClass = this.getRegisteredEClassforEClassViaEPackage(eReference.getEContainingClass());
		if (eClass == null) {
			// we don't know the owning EClass, maybe the EPackage was not registered?
			return null;
		}
		EStructuralFeature feature = eClass.getEStructuralFeature(eReference.getName());
		if (feature == null) {
			// in our version of the EClass, no such feature exists...
			return null;
		}
		if (feature instanceof EReference == false) {
			// in our version of the eClass, it's an EAttribute...
			return null;
		}
		EReference registeredReference = (EReference) feature;
		return this.eReferenceToID.get(registeredReference);
	}

	// =================================================================================================================
	// INTERNAL API
	// =================================================================================================================

	@Override
	public void registerEPackage(final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		this.assertNotSealed();
		if (ePackage.getNsURI() == null || ePackage.getNsURI().trim().isEmpty()) {
			throw new IllegalArgumentException("Cannot store an EPackage that has no Namespace URI (NSURI)!");
		}
		EPackage existingEPackage = this.nsURItoEPackage.put(ePackage.getNsURI(), ePackage);
		if (existingEPackage != null) {
			ChronoLogger.logWarning("Registration of EPackage with Namespace URI '" + ePackage.getNsURI()
					+ "' overrides an existing EPackage!");
		}
	}

	@Override
	public void registerEClassID(final EClass eClass, final String chronoEClassID) {
		this.assertNotSealed();
		String previousID = this.eClassToID.get(eClass);
		if (previousID != null) {
			if (previousID.equals(chronoEClassID) == false) {
				throw new IllegalStateException("EClass '" + eClass.getName() + "' already has an assigned ID of '"
						+ previousID + "', can't assign ID '" + chronoEClassID + "'!");
			} else {
				// same id has already been assigned
				return;
			}
		}
		this.eClassToID.put(eClass, chronoEClassID);
	}

	@Override
	public void registerEAttributeID(final EAttribute eAttribute, final String chronoEAttributeID) {
		this.assertNotSealed();
		String previousID = this.eAttributeToID.get(eAttribute);
		if (previousID != null) {
			if (previousID.equals(chronoEAttributeID) == false) {
				throw new IllegalStateException(
						"EAttribute '" + eAttribute.getName() + "' already has an assigned ID of '" + previousID
								+ "', can't assign ID '" + chronoEAttributeID + "'!");
			} else {
				// same id has already been assigned
				return;
			}
		}
		this.eAttributeToID.put(eAttribute, chronoEAttributeID);
	}

	@Override
	public void registerEReferenceID(final EReference eReference, final String chronoEReferenceID) {
		this.assertNotSealed();
		String previousID = this.eReferenceToID.get(eReference);
		if (previousID != null) {
			if (previousID.equals(chronoEReferenceID) == false) {
				throw new IllegalStateException(
						"EAttribute '" + eReference.getName() + "' already has an assigned ID of '" + previousID
								+ "', can't assign ID '" + chronoEReferenceID + "'!");
			} else {
				// same id has already been assigned
				return;
			}
		}
		this.eReferenceToID.put(eReference, chronoEReferenceID);
	}

	@Override
	public Set<EPackage> getEPackages() {
		return Sets.newHashSet(this.nsURItoEPackage.values());
	}

	@Override
	public void seal() {
		if (this.isSealed) {
			return;
		}
		// register the correct EFactory instance at all (sub-)packages
		Stack<EPackage> ePackagesToVisit = new Stack<>();
		for (EPackage ePackage : this.nsURItoEPackage.values()) {
			ePackagesToVisit.push(ePackage);
		}
		Set<EPackage> visitedEPackages = Sets.newHashSet();
		while (ePackagesToVisit.isEmpty() == false) {
			EPackage pack = ePackagesToVisit.pop();
			if (visitedEPackages.contains(pack)) {
				continue;
			}
			visitedEPackages.add(pack);
			pack.setEFactoryInstance(new ChronoEFactory());
			for (EPackage subPackage : pack.getESubpackages()) {
				ePackagesToVisit.push(subPackage);
			}
		}
		this.isSealed = true;
	}

	// =================================================================================================================
	// HELPER METHODS
	// =================================================================================================================

	private void assertNotSealed() {
		if (this.isSealed) {
			throw new IllegalStateException(
					"ChronoEPackage is already sealed off! No modifications are allowed anymore.");
		}
	}

	private EClass getRegisteredEClassforEClassViaEPackage(final EClass eClass) {
		EPackage registeredEPackage = this.getEPackage(eClass.getEPackage().getNsURI());
		if (registeredEPackage == null) {
			// the ePackage is not registered...
			return null;
		}
		EClass registeredClass = (EClass) registeredEPackage.getEClassifier(eClass.getName());
		return registeredClass;
	}

}
