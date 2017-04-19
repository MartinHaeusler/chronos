package org.chronos.chronosphere.testmodels.meta;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.EcorePackage;

public class PersonMetamodel {

	public static final String PERSON_EPACKAGE_NS_URI = "http://www.example.com/model/person";

	public static EPackage createPersonEPackage() {
		EPackage ePackage = EcoreFactory.eINSTANCE.createEPackage();
		ePackage.setNsURI(PERSON_EPACKAGE_NS_URI);
		ePackage.setNsPrefix("http://www.example.com/model");
		ePackage.setName("Person");
		{
			EClass ecPerson = EcoreFactory.eINSTANCE.createEClass();
			ecPerson.setName("Person");
			{
				EAttribute eaFirstName = EcoreFactory.eINSTANCE.createEAttribute();
				eaFirstName.setName("firstName");
				eaFirstName.setEType(EcorePackage.Literals.ESTRING);
				eaFirstName.setLowerBound(0);
				eaFirstName.setUpperBound(1);
				ecPerson.getEStructuralFeatures().add(eaFirstName);

				EAttribute eaLastName = EcoreFactory.eINSTANCE.createEAttribute();
				eaLastName.setName("lastName");
				eaLastName.setEType(EcorePackage.Literals.ESTRING);
				eaLastName.setLowerBound(0);
				eaLastName.setUpperBound(1);
				ecPerson.getEStructuralFeatures().add(eaLastName);

				EReference erFriend = EcoreFactory.eINSTANCE.createEReference();
				erFriend.setName("friend");
				erFriend.setEType(ecPerson);
				erFriend.setLowerBound(0);
				erFriend.setUpperBound(-1);
				erFriend.setOrdered(false);
				erFriend.setUnique(true);
				erFriend.setContainment(false);
				ecPerson.getEStructuralFeatures().add(erFriend);

				EReference erMarried = EcoreFactory.eINSTANCE.createEReference();
				erMarried.setName("married");
				erMarried.setEType(ecPerson);
				erMarried.setEOpposite(erMarried);
				erMarried.setLowerBound(0);
				erMarried.setUpperBound(1);
				erMarried.setContainment(false);
				ecPerson.getEStructuralFeatures().add(erMarried);

				EReference erChild = EcoreFactory.eINSTANCE.createEReference();
				erChild.setName("child");
				erChild.setEType(ecPerson);
				erChild.setLowerBound(0);
				erChild.setUpperBound(-1);
				erChild.setUnique(true);
				erChild.setOrdered(false);
				ecPerson.getEStructuralFeatures().add(erChild);

			}
			ePackage.getEClassifiers().add(ecPerson);
		}
		return ePackage;
	}

}
