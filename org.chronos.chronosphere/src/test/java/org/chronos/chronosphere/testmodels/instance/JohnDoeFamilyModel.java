package org.chronos.chronosphere.testmodels.instance;

import static org.junit.Assert.*;

import org.chronos.chronosphere.testmodels.meta.PersonMetamodel;
import org.chronos.chronosphere.testutils.EMFTestUtils;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

public class JohnDoeFamilyModel extends AbstractTestModel {

	public static final String ID_JOHN_DOE = "4145c03e-d324-498a-a02b-0db5ed566019";
	public static final String ID_JANE_DOE = "ba0ca4f4-c431-49c7-9292-a03fe0c449a5";
	public static final String ID_JACK_SMITH = "87f231e4-eb41-477c-bd7f-60ad686c6e35";
	public static final String ID_JOHN_PARKER = "98587546-92a0-4a75-855d-043c36184fc5";
	public static final String ID_SARAH_DOE = "16556ba6-1339-4861-acbd-b8eb4d76c0be";

	public JohnDoeFamilyModel() {
		super(PersonMetamodel.createPersonEPackage());
	}

	// =====================================================================================================================
	// MODEL CREATION
	// =====================================================================================================================

	@Override
	protected void createModelData() {
		EPackage ePackage = this.getEPackage();
		// extract some data from the EPackage
		EClass ecPerson = (EClass) ePackage.getEClassifier("Person");
		assertNotNull(ecPerson);
		EAttribute eaFirstName = (EAttribute) ecPerson.getEStructuralFeature("firstName");
		EAttribute eaLastName = (EAttribute) ecPerson.getEStructuralFeature("lastName");
		EReference erFriend = (EReference) ecPerson.getEStructuralFeature("friend");
		EReference erMarried = (EReference) ecPerson.getEStructuralFeature("married");
		EReference erChild = (EReference) ecPerson.getEStructuralFeature("child");
		assertNotNull(eaFirstName);
		assertNotNull(eaLastName);
		assertNotNull(erFriend);
		assertNotNull(erMarried);
		assertNotNull(erChild);

		// create some persons
		EObject pJohnDoe = this.createAndRegisterEObject(ID_JOHN_DOE, ecPerson);
		pJohnDoe.eSet(eaFirstName, "John");
		pJohnDoe.eSet(eaLastName, "Doe");

		EObject pJaneDoe = this.createAndRegisterEObject(ID_JANE_DOE, ecPerson);
		pJaneDoe.eSet(eaFirstName, "Jane");
		pJaneDoe.eSet(eaLastName, "Doe");

		// marry john and jane
		pJohnDoe.eSet(erMarried, pJaneDoe);
		// assert that the inverse was set
		assertEquals(pJohnDoe, pJaneDoe.eGet(erMarried));

		EObject pJackSmith = this.createAndRegisterEObject(ID_JACK_SMITH, ecPerson);
		pJackSmith.eSet(eaFirstName, "Jack");
		pJackSmith.eSet(eaLastName, "Smith");

		// john and jack are friends
		EMFTestUtils.addToEReference(pJohnDoe, erFriend, pJackSmith);
		EMFTestUtils.addToEReference(pJackSmith, erFriend, pJohnDoe);

		EObject pJohnParker = this.createAndRegisterEObject(ID_JOHN_PARKER, ecPerson);
		pJohnParker.eSet(eaFirstName, "John");
		pJohnParker.eSet(eaLastName, "Parker");

		// john d. and john p. are also friends
		EMFTestUtils.addToEReference(pJohnDoe, erFriend, pJohnParker);
		EMFTestUtils.addToEReference(pJohnParker, erFriend, pJohnDoe);

		EObject pSarahDoe = this.createAndRegisterEObject(ID_SARAH_DOE, ecPerson);
		pSarahDoe.eSet(eaFirstName, "Sarah");
		pSarahDoe.eSet(eaLastName, "Doe");

		// sarah is the child of john and jane
		EMFTestUtils.addToEReference(pJohnDoe, erChild, pSarahDoe);
		EMFTestUtils.addToEReference(pJaneDoe, erChild, pSarahDoe);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public EObject getJohnDoe() {
		return this.getEObjectByID(ID_JOHN_DOE);
	}

	public EObject getJaneDoe() {
		return this.getEObjectByID(ID_JANE_DOE);
	}

	public EObject getSarahDoe() {
		return this.getEObjectByID(ID_SARAH_DOE);
	}

	public EObject getJackSmith() {
		return this.getEObjectByID(ID_JACK_SMITH);
	}

	public EObject getJohnParker() {
		return this.getEObjectByID(ID_JOHN_PARKER);
	}

}
