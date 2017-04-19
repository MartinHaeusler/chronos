package org.chronos.chronosphere.test.emf.estore.impl;

import static org.junit.Assert.*;

import java.util.List;

import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.test.emf.estore.base.EStoreTest;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.EcorePackage;
import org.junit.Test;

public class BasicEStoreTest extends EStoreTest {

	// =================================================================================================================
	// CROSS EREFERENCES TESTS (NON-CONTAINMENT)
	// =================================================================================================================

	@Test
	public void multiplicityOneCrossRefWithoutOppositeRefWorks() {
		this.createEPackageMultiplicityOneCrossRefNoOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EReference childRef = (EReference) myClass.getEStructuralFeature("child");
		assertNotNull(childRef);
		EObject eObj1 = this.createEObject(myClass);
		EObject eObj2 = this.createEObject(yourClass);

		// set eObj2 as the child of eObj1
		eObj1.eSet(childRef, eObj2);
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());

		// unSet the child reference in eObj1
		eObj1.eUnset(childRef);
		assertFalse(eObj1.eIsSet(childRef));
		assertNull(eObj1.eGet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());

		// set eObj2 as the child of eObj1 again
		eObj1.eSet(childRef, eObj2);
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());

		// this time, clear the child reference in eObj1 by assigning NULL
		eObj1.eSet(childRef, null);
		assertNull(eObj1.eGet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
	}

	@Test
	public void multiplicityOneCrossRefWithMultiplicityOneOppositeRefWorks() {
		this.createEPackageMultiplicityOneCrossRefWithMultiplicityOneOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EReference childRef = (EReference) myClass.getEStructuralFeature("child");
		assertNotNull(childRef);
		EReference parentRef = (EReference) yourClass.getEStructuralFeature("parent");
		assertNotNull(parentRef);
		EObject eObj1 = this.createEObject(myClass);
		EObject eObj2 = this.createEObject(yourClass);

		// set eObj2 as the child of eObj1
		eObj1.eSet(childRef, eObj2);
		assertTrue(eObj1.eIsSet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
		assertTrue(eObj2.eIsSet(parentRef));
		assertEquals(eObj1, eObj2.eGet(parentRef));

		// unSet the child reference in eObj1
		eObj1.eUnset(childRef);
		assertFalse(eObj1.eIsSet(childRef));
		assertNull(eObj1.eGet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
		assertFalse(eObj2.eIsSet(parentRef));
		assertNull(eObj2.eGet(parentRef));

		// set eObj2 as the child of eObj1 again
		eObj1.eSet(childRef, eObj2);
		assertTrue(eObj1.eIsSet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
		assertTrue(eObj2.eIsSet(parentRef));
		assertEquals(eObj1, eObj2.eGet(parentRef));

		// this time, clear the child reference in eObj1 by assigning NULL
		eObj1.eSet(childRef, null);
		assertFalse(eObj1.eIsSet(childRef)); // according to Ecore reference impl, this has to be false
		assertNull(eObj1.eGet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
		assertFalse(eObj2.eIsSet(parentRef));
		assertNull(eObj2.eGet(parentRef));

		// set eObj1 as the parent of eObj2
		eObj2.eSet(parentRef, eObj1);
		assertTrue(eObj1.eIsSet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
		assertTrue(eObj2.eIsSet(parentRef));
		assertEquals(eObj1, eObj2.eGet(parentRef));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void multiplicityOneCrossRefWithMultiplicityManyOppositeRefWorks() {
		this.createEPackageMultiplicityOneCrossRefWithMultiplicityManyOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EReference childRef = (EReference) myClass.getEStructuralFeature("child");
		assertNotNull(childRef);
		EReference parentRef = (EReference) yourClass.getEStructuralFeature("parents");
		assertNotNull(parentRef);
		EObject parent1 = ePackage.getEFactoryInstance().create(myClass);
		EObject parent2 = ePackage.getEFactoryInstance().create(myClass);
		EObject child = ePackage.getEFactoryInstance().create(yourClass);

		// attach the parents to the child via the "child" references
		parent1.eSet(childRef, child);
		parent2.eSet(childRef, child);
		assertEquals(child, parent1.eGet(childRef));
		assertEquals(child, parent2.eGet(childRef));
		assertTrue(((List<EObject>) child.eGet(parentRef)).contains(parent1));
		assertTrue(((List<EObject>) child.eGet(parentRef)).contains(parent2));
		assertTrue(parent1.eIsSet(childRef));
		assertTrue(parent2.eIsSet(childRef));
		assertTrue(child.eIsSet(parentRef));
		assertNull(child.eContainer());
		assertNull(child.eContainingFeature());

		// remove one of the parents
		((List<EObject>) child.eGet(parentRef)).remove(parent1);
		assertFalse(parent1.eIsSet(childRef));
		assertTrue(parent2.eIsSet(childRef));
		assertEquals(null, parent1.eGet(childRef));
		assertEquals(child, parent2.eGet(childRef));
		assertFalse(((List<EObject>) child.eGet(parentRef)).contains(parent1));
		assertTrue(((List<EObject>) child.eGet(parentRef)).contains(parent2));
		assertNull(child.eContainer());
		assertNull(child.eContainingFeature());

		// add the parent again by adding it to the "parents" reference
		((List<EObject>) child.eGet(parentRef)).add(parent1);
		assertTrue(parent1.eIsSet(childRef));
		assertTrue(parent2.eIsSet(childRef));
		assertEquals(child, parent1.eGet(childRef));
		assertEquals(child, parent2.eGet(childRef));
		assertTrue(((List<EObject>) child.eGet(parentRef)).contains(parent1));
		assertTrue(((List<EObject>) child.eGet(parentRef)).contains(parent2));
		assertNull(child.eContainer());
		assertNull(child.eContainingFeature());

		// remove the second parent by setting its child to null
		parent2.eSet(childRef, null);
		assertTrue(parent1.eIsSet(childRef));
		assertFalse(parent2.eIsSet(childRef));
		assertEquals(child, parent1.eGet(childRef));
		assertEquals(null, parent2.eGet(childRef));
		assertTrue(((List<EObject>) child.eGet(parentRef)).contains(parent1));
		assertFalse(((List<EObject>) child.eGet(parentRef)).contains(parent2));
		assertNull(child.eContainer());
		assertNull(child.eContainingFeature());

		// remove the first parent by unsetting its child reference
		parent1.eUnset(childRef);
		assertFalse(parent1.eIsSet(childRef));
		assertFalse(parent2.eIsSet(childRef));
		assertEquals(null, parent1.eGet(childRef));
		assertEquals(null, parent2.eGet(childRef));
		assertFalse(((List<EObject>) child.eGet(parentRef)).contains(parent1));
		assertFalse(((List<EObject>) child.eGet(parentRef)).contains(parent2));
		assertNull(child.eContainer());
		assertNull(child.eContainingFeature());

		// add the parents again
		parent1.eSet(childRef, child);
		parent2.eSet(childRef, child);
		assertEquals(child, parent1.eGet(childRef));
		assertEquals(child, parent2.eGet(childRef));
		assertTrue(((List<EObject>) child.eGet(parentRef)).contains(parent1));
		assertTrue(((List<EObject>) child.eGet(parentRef)).contains(parent2));
		assertTrue(parent1.eIsSet(childRef));
		assertTrue(parent2.eIsSet(childRef));
		assertTrue(child.eIsSet(parentRef));
		assertNull(child.eContainer());
		assertNull(child.eContainingFeature());

		// remove the parents by clearing the "parents" reference
		child.eUnset(parentRef);
		assertFalse(parent1.eIsSet(childRef));
		assertFalse(parent2.eIsSet(childRef));
		assertEquals(null, parent1.eGet(childRef));
		assertEquals(null, parent2.eGet(childRef));
		assertFalse(((List<EObject>) child.eGet(parentRef)).contains(parent1));
		assertFalse(((List<EObject>) child.eGet(parentRef)).contains(parent2));
		assertNull(child.eContainer());
		assertNull(child.eContainingFeature());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void multiplicityManyCrossRefWithoutOppositeRefWorks() {
		this.createEPackageMultiplicityManyCrossRefNoOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EReference childRef = (EReference) myClass.getEStructuralFeature("children");
		assertNotNull(childRef);
		EObject parent = this.createEObject(myClass);
		EObject child1 = this.createEObject(yourClass);
		EObject child2 = this.createEObject(yourClass);

		// add the children to the parent
		((List<EObject>) parent.eGet(childRef)).add(child1);
		((List<EObject>) parent.eGet(childRef)).add(child2);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// remove child1
		((List<EObject>) parent.eGet(childRef)).remove(child1);
		assertTrue(parent.eIsSet(childRef));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// re-add child1
		((List<EObject>) parent.eGet(childRef)).add(child1);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// clear the children reference
		((List<EObject>) parent.eGet(childRef)).clear();
		assertFalse(parent.eIsSet(childRef)); // according to standard Ecore implementation
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// add the children to the parent
		((List<EObject>) parent.eGet(childRef)).add(child1);
		((List<EObject>) parent.eGet(childRef)).add(child2);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// unset the children reference
		parent.eUnset(childRef);
		assertFalse(parent.eIsSet(childRef));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// add the children to the parent
		((List<EObject>) parent.eGet(childRef)).add(child1);
		((List<EObject>) parent.eGet(childRef)).add(child2);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// remove the children one by one
		((List<EObject>) parent.eGet(childRef)).remove(child1);
		((List<EObject>) parent.eGet(childRef)).remove(child2);
		assertFalse(parent.eIsSet(childRef)); // according to Ecore implementation
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void multiplicityManyCrossRefWithMultiplicityOneOppositeRefWorks() {
		this.createEPackageMultiplicityManyCrossRefWithMultiplicityOneOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EReference childRef = (EReference) myClass.getEStructuralFeature("children");
		assertNotNull(childRef);
		EReference parentRef = (EReference) yourClass.getEStructuralFeature("parent");
		assertNotNull(parentRef);
		EObject parent = this.createEObject(myClass);
		EObject child1 = this.createEObject(yourClass);
		EObject child2 = this.createEObject(yourClass);

		// add the children to the parent by adding them to the "children" reference
		((List<EObject>) parent.eGet(childRef)).add(child1);
		((List<EObject>) parent.eGet(childRef)).add(child2);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertTrue(child1.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertTrue(child2.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// remove child1 by removing it from the children list
		((List<EObject>) parent.eGet(childRef)).remove(child1);
		assertTrue(parent.eIsSet(childRef));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertFalse(child1.eIsSet(parentRef));
		assertEquals(null, child1.eGet(parentRef));
		assertTrue(child2.eIsSet(parentRef));
		assertEquals(parent, child2.eGet(parentRef));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// re-add child1 by setting the parent reference
		child1.eSet(parentRef, parent);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertTrue(child1.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertTrue(child2.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// clear the children reference
		((List<EObject>) parent.eGet(childRef)).clear();
		assertFalse(parent.eIsSet(childRef)); // according to standard Ecore implementation
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertFalse(child1.eIsSet(parentRef));
		assertNull(child1.eGet(parentRef));
		assertFalse(child2.eIsSet(parentRef));
		assertNull(child1.eGet(parentRef));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// add the children to the parent
		((List<EObject>) parent.eGet(childRef)).add(child1);
		((List<EObject>) parent.eGet(childRef)).add(child2);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertTrue(child1.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertTrue(child2.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// unset the parent reference in child2
		child2.eUnset(parentRef);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertTrue(child1.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertFalse(child2.eIsSet(parentRef));
		assertEquals(null, child2.eGet(parentRef));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// add child2 to the parent by adding it to the children reference
		((List<EObject>) parent.eGet(childRef)).add(child2);
		assertTrue(parent.eIsSet(childRef));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertTrue(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertTrue(child1.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertTrue(child2.eIsSet(parentRef));
		assertEquals(parent, child1.eGet(parentRef));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());

		// unset the children reference
		parent.eUnset(childRef);
		assertFalse(parent.eIsSet(childRef));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child1));
		assertFalse(((List<EObject>) parent.eGet(childRef)).contains(child2));
		assertFalse(child1.eIsSet(parentRef));
		assertNull(child1.eGet(parentRef));
		assertFalse(child2.eIsSet(parentRef));
		assertNull(child2.eGet(parentRef));
		assertNull(child1.eContainer());
		assertNull(child1.eContainingFeature());
		assertNull(child2.eContainer());
		assertNull(child2.eContainingFeature());
	}

	@Test
	public void symmetricMultiplicityManyCrossRefWorks() {
		this.createEPackageSymmetricMultiplicityManyCrosssRef();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		assertNotNull(ePackage);
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		assertNotNull(myClass);
		EReference eRef = (EReference) myClass.getEStructuralFeature("ref");
		assertNotNull(eRef);

		EObject eObj1 = this.createEObject(myClass);
		EObject eObj2 = this.createEObject(myClass);
		EObject eObj3 = this.createEObject(myClass);

		List<EObject> targets = EMFUtils.eGetMany(eObj1, eRef);
		targets.add(eObj2);
		targets.add(eObj3);

		// since "ref" is symmetric, eobj1-[ref]->eobj2 implies that eobj2-[ref]->eobj1
		assertTrue(EMFUtils.eGetMany(eObj2, eRef).contains(eObj1));
		// since "ref" is symmetric, eobj1-[ref]->eobj3 implies that eobj3-[ref]->eobj1
		assertTrue(EMFUtils.eGetMany(eObj3, eRef).contains(eObj1));
		// since "ref" is not transitive, eobj3 and eobj2 should not know each other
		assertFalse(EMFUtils.eGetMany(eObj2, eRef).contains(eObj3));
		assertFalse(EMFUtils.eGetMany(eObj3, eRef).contains(eObj2));
	}

	// =================================================================================================================
	// CONTAINMENT EREFERENCE TESTS
	// =================================================================================================================

	@Test
	public void multiplicityOneContainmentWithoutOppositeRefWorks() {
		this.createEPackageMultiplicityOneContainmentNoOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EReference childRef = (EReference) myClass.getEStructuralFeature("child");
		assertNotNull(childRef);
		EObject eObj1 = this.createEObject(myClass);
		EObject eObj2 = this.createEObject(yourClass);

		// set eObj2 as the child of eObj1
		eObj1.eSet(childRef, eObj2);
		assertEquals(eObj1, eObj2.eContainer());
		assertEquals(childRef, eObj2.eContainingFeature());

		// unSet the child reference in eObj1
		eObj1.eUnset(childRef);
		assertFalse(eObj1.eIsSet(childRef));
		assertNull(eObj1.eGet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());

		// set eObj2 as the child of eObj1 again
		eObj1.eSet(childRef, eObj2);
		assertEquals(eObj1, eObj2.eContainer());
		assertEquals(childRef, eObj2.eContainingFeature());

		// this time, clear the child reference in eObj1 by assigning NULL
		eObj1.eSet(childRef, null);
		assertNull(eObj1.eGet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
	}

	@Test
	public void multiplicityOneContainmentWithOppositeRefWorks() {
		this.createEPackageMultiplicityOneContainmentWithOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EReference childRef = (EReference) myClass.getEStructuralFeature("child");
		assertNotNull(childRef);
		EReference parentRef = (EReference) yourClass.getEStructuralFeature("parent");
		assertNotNull(parentRef);
		EObject eObj1 = this.createEObject(myClass);
		EObject eObj2 = this.createEObject(yourClass);

		// set eObj2 as the child of eObj1
		eObj1.eSet(childRef, eObj2);
		assertTrue(eObj1.eIsSet(childRef));
		assertEquals(eObj1, eObj2.eContainer());
		assertEquals(childRef, eObj2.eContainingFeature());
		assertTrue(eObj2.eIsSet(parentRef));
		assertEquals(eObj1, eObj2.eGet(parentRef));

		// unSet the child reference in eObj1
		eObj1.eUnset(childRef);
		assertFalse(eObj1.eIsSet(childRef));
		assertNull(eObj1.eGet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
		assertNull(eObj2.eGet(parentRef));

		// set eObj2 as the child of eObj1 again
		eObj1.eSet(childRef, eObj2);
		assertTrue(eObj1.eIsSet(childRef));
		assertEquals(eObj1, eObj2.eContainer());
		assertEquals(childRef, eObj2.eContainingFeature());
		assertTrue(eObj2.eIsSet(parentRef));
		assertEquals(eObj1, eObj2.eGet(parentRef));

		// this time, clear the child reference in eObj1 by assigning NULL
		eObj1.eSet(childRef, null);
		assertFalse(eObj1.eIsSet(childRef)); // according to Ecore reference impl, this has to be false
		assertNull(eObj1.eGet(childRef));
		assertNull(eObj2.eContainer());
		assertNull(eObj2.eContainingFeature());
		assertFalse(eObj2.eIsSet(parentRef));
		assertNull(eObj2.eGet(parentRef));

		// set eObj1 as the parent of eObj2
		eObj2.eSet(parentRef, eObj1);
		assertTrue(eObj1.eIsSet(childRef));
		assertEquals(eObj1, eObj2.eContainer());
		assertEquals(childRef, eObj2.eContainingFeature());
		assertTrue(eObj2.eIsSet(parentRef));
		assertEquals(eObj1, eObj2.eGet(parentRef));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void multiplicityManyContainmentWithoutOppositeRefWorks() {
		this.createEPackageMultiplicityManyContainmentNoOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EAttribute myAttribute = (EAttribute) myClass.getEStructuralFeature("MyAttribute");
		EReference myReference = (EReference) myClass.getEStructuralFeature("children");
		EObject eObj1 = this.createEObject(myClass);
		EObject eObj2 = this.createEObject(yourClass);

		// assign a property to eObj1
		assertNull(eObj1.eGet(myAttribute));
		eObj1.eSet(myAttribute, "Hello World!");
		assertEquals("Hello World!", eObj1.eGet(myAttribute));

		// assign a reference target to eObj1
		((List<EObject>) eObj1.eGet(myReference)).add(eObj2);

		// get the EContainer of eObj2
		assertEquals(eObj1, eObj2.eContainer());

		// get the eContainingFeature of eObj2
		assertEquals(myReference, eObj2.eContainingFeature());

		// assert that our children reference was updated
		assertTrue(((List<EObject>) eObj1.eGet(myReference)).contains(eObj2));

		// clear the list in eObj1#children
		((List<EObject>) eObj1.eGet(myReference)).clear();

		// get the EContainer of eObj2
		assertNull(eObj2.eContainer());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void multiplicityManyContainmentWithMultiplicityOneOppositeRefWorks() {
		this.createEPackageMultiplicityManyContainmentWithMultiplicityOneOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass myClass = (EClass) ePackage.getEClassifier("MyEClass");
		EClass yourClass = (EClass) ePackage.getEClassifier("YourEClass");
		assertNotNull(myClass);
		assertNotNull(yourClass);
		EReference eRefChildren = (EReference) myClass.getEStructuralFeature("children");
		EReference eRefParent = (EReference) yourClass.getEStructuralFeature("parent");
		assertNotNull(eRefChildren);
		assertNotNull(eRefParent);
		assertEquals(eRefParent, eRefChildren.getEOpposite());
		assertEquals(eRefChildren, eRefParent.getEOpposite());
		assertTrue(eRefChildren.isContainment());
		assertTrue(eRefParent.isContainer());
		EObject container = this.createEObject(myClass);
		EObject child1 = this.createEObject(yourClass);
		EObject child2 = this.createEObject(yourClass);

		// add a child to the container by adding it to the children reference
		((List<EObject>) container.eGet(eRefChildren)).add(child1);

		// add a child to the container by setting the container as parent in the child
		child2.eSet(eRefParent, container);

		// make sure that both children are referencing the container as parent
		assertEquals(container, child1.eContainer());
		assertEquals(container, child2.eContainer());
		assertTrue(child1.eIsSet(eRefParent));
		assertTrue(child2.eIsSet(eRefParent));
		assertEquals(container, child1.eGet(eRefParent));
		assertEquals(container, child2.eGet(eRefParent));
		assertEquals(eRefChildren, child1.eContainingFeature());
		assertEquals(eRefChildren, child2.eContainingFeature());

		// remove the first child from the container by unsetting the parent reference
		child1.eUnset(eRefParent);

		// make sure that it's no longer part of the container
		assertNull(child1.eContainer());
		assertFalse(((List<EObject>) container.eGet(eRefChildren)).contains(child1));

		// remove the second child from the container by removing it from the children list
		((List<EObject>) container.eGet(eRefChildren)).remove(child2);

		// assert that its parent reference has been unset
		assertNull(child2.eGet(eRefParent));
		assertFalse(child2.eIsSet(eRefParent));
	}

	@Test
	public void inheritedMultiplicityManyContainmentWithNonInheritedMultiplicityOneOppositeWorks() {
		this.createEPackageInheritedMultiplicityManyContainmentWithNonInheritedMultiplicityOneOpposite();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		EClass ecContainer = (EClass) ePackage.getEClassifier("Container");
		assertNotNull(ecContainer);
		EClass ecElement = (EClass) ePackage.getEClassifier("Element");
		assertNotNull(ecElement);

		EReference erElements = (EReference) ecContainer.getEStructuralFeature("elements");
		EReference erContainer = (EReference) ecElement.getEStructuralFeature("container");
		assertNotNull(erElements);
		assertNotNull(erContainer);
		assertEquals(erContainer, erElements.getEOpposite());
		assertEquals(erElements, erContainer.getEOpposite());
		assertTrue(erElements.isContainment());
		assertTrue(erContainer.isContainer());

		EObject container = this.createEObject(ecContainer);
		EObject child1 = this.createEObject(ecElement);
		EObject child2 = this.createEObject(ecElement);

		// add a child to the container by adding it to the children reference
		EMFUtils.eGetMany(container, erElements).add(child1);

		// add a child to the container by setting the container as parent in the child
		child2.eSet(erContainer, container);

		// make sure that both children are referencing the container as parent
		assertEquals(container, child1.eContainer());
		assertEquals(container, child2.eContainer());
		assertTrue(child1.eIsSet(erContainer));
		assertTrue(child2.eIsSet(erContainer));
		assertEquals(container, child1.eGet(erContainer));
		assertEquals(container, child2.eGet(erContainer));
		assertEquals(erElements, child1.eContainingFeature());
		assertEquals(erElements, child2.eContainingFeature());

		// remove the first child from the container by unsetting the parent reference
		child1.eUnset(erContainer);

		// make sure that it's no longer part of the container
		assertNull(child1.eContainer());
		assertFalse(EMFUtils.eGetMany(container, erElements).contains(child1));

		// remove the second child from the container by removing it from the children list
		EMFUtils.eGetMany(container, erElements).remove(child2);

		// assert that its parent reference has been unset
		assertNull(child2.eGet(erContainer));
		assertFalse(child2.eIsSet(erContainer));

	}

	// =====================================================================================================================
	// GRABATS TESTS
	// =====================================================================================================================

	@Test
	public void grabatsFragmantModelWorks() {
		// the following test uses a fragment of the 'JDTAST.ecore' model (from GRABATS).
		this.createGrabatsFragmentsEPackage();
		EPackage ePackage = this.getEPackageByNsURI("http://www.example.com/model");
		// open a transaction and create some EObject instances based on the EPackage
		EClass ecIPackageFragment = (EClass) ePackage.getEClassifier("IPackageFragment");
		assertNotNull(ecIPackageFragment);
		EClass ecBinaryPackageFragmentRoot = (EClass) ePackage.getEClassifier("BinaryPackageFragmentRoot");
		assertNotNull(ecBinaryPackageFragmentRoot);

		EReference erPackageFragments = (EReference) ecBinaryPackageFragmentRoot
				.getEStructuralFeature("packageFragments");
		assertNotNull(erPackageFragments);

		EReference erPackageFragmentRoot = (EReference) ecIPackageFragment.getEStructuralFeature("packageFragmentRoot");
		assertNotNull(erPackageFragmentRoot);

		EAttribute eaElementName = (EAttribute) ecBinaryPackageFragmentRoot.getEStructuralFeature("elementName");
		assertNotNull(eaElementName);

		EAttribute eaPath = (EAttribute) ecIPackageFragment.getEStructuralFeature("path");
		assertNotNull(eaPath);

		EAttribute eaIsReadOnly = (EAttribute) ecIPackageFragment.getEStructuralFeature("isReadOnly");
		assertNotNull(eaIsReadOnly);

		// create a model and attach it
		EObject packageRoot = this.createEObject(ecBinaryPackageFragmentRoot);
		packageRoot.eSet(eaElementName, "root");
		packageRoot.eSet(eaPath, "/");
		packageRoot.eSet(eaIsReadOnly, true);

		EObject frag1 = this.createEObject(ecIPackageFragment);
		frag1.eSet(eaElementName, "Frag1");
		frag1.eSet(eaIsReadOnly, true);
		frag1.eSet(eaPath, ".frag1");
		EMFUtils.eGetMany(packageRoot, erPackageFragments).add(frag1);

		assertEquals(packageRoot, frag1.eGet(erPackageFragmentRoot));
	}

	// =====================================================================================================================
	// EPACKAGES
	// =====================================================================================================================

	private void createEPackageMultiplicityOneCrossRefNoOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EReference eReference = EcoreFactory.eINSTANCE.createEReference();
		eReference.setName("child");
		eReference.setEType(eOtherClass);
		eReference.setLowerBound(0);
		eReference.setUpperBound(1);
		eReference.setContainment(false);
		eClass.getEStructuralFeatures().add(eReference);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageMultiplicityOneCrossRefWithMultiplicityOneOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EReference eRefChild = EcoreFactory.eINSTANCE.createEReference();
		eRefChild.setName("child");
		eRefChild.setEType(eOtherClass);
		eRefChild.setLowerBound(0);
		eRefChild.setUpperBound(1);
		eRefChild.setContainment(false);
		EReference eRefParent = EcoreFactory.eINSTANCE.createEReference();
		eRefParent.setName("parent");
		eRefParent.setLowerBound(0);
		eRefParent.setUpperBound(1);
		eRefParent.setEType(eClass);
		eRefParent.setEOpposite(eRefChild);
		eRefChild.setEOpposite(eRefParent);
		eClass.getEStructuralFeatures().add(eRefChild);
		eOtherClass.getEStructuralFeatures().add(eRefParent);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageMultiplicityOneCrossRefWithMultiplicityManyOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EReference eRefChild = EcoreFactory.eINSTANCE.createEReference();
		eRefChild.setName("child");
		eRefChild.setEType(eOtherClass);
		eRefChild.setLowerBound(0);
		eRefChild.setUpperBound(1);
		eRefChild.setContainment(false);
		EReference eRefParent = EcoreFactory.eINSTANCE.createEReference();
		eRefParent.setName("parents");
		eRefParent.setLowerBound(0);
		eRefParent.setUpperBound(-1);
		eRefParent.setEType(eClass);
		eRefParent.setEOpposite(eRefChild);
		eRefChild.setEOpposite(eRefParent);
		eClass.getEStructuralFeatures().add(eRefChild);
		eOtherClass.getEStructuralFeatures().add(eRefParent);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageMultiplicityManyCrossRefNoOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EReference eReference = EcoreFactory.eINSTANCE.createEReference();
		eReference.setName("children");
		eReference.setEType(eOtherClass);
		eReference.setLowerBound(0);
		eReference.setUpperBound(-1);
		eReference.setContainment(false);
		eClass.getEStructuralFeatures().add(eReference);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageMultiplicityManyCrossRefWithMultiplicityOneOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EReference eRefChildren = EcoreFactory.eINSTANCE.createEReference();
		eRefChildren.setName("children");
		eRefChildren.setEType(eOtherClass);
		eRefChildren.setLowerBound(0);
		eRefChildren.setUpperBound(-1);
		eRefChildren.setContainment(false);
		eClass.getEStructuralFeatures().add(eRefChildren);
		EReference eRefParent = EcoreFactory.eINSTANCE.createEReference();
		eRefParent.setName("parent");
		eRefParent.setLowerBound(0);
		eRefParent.setUpperBound(1);
		eRefParent.setEType(eClass);
		eRefParent.setEOpposite(eRefChildren);
		eRefChildren.setEOpposite(eRefParent);
		eOtherClass.getEStructuralFeatures().add(eRefParent);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageSymmetricMultiplicityManyCrosssRef() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		{
			EClass eClass = EcoreFactory.eINSTANCE.createEClass();
			eClass.setName("MyEClass");
			{
				EReference eRef = EcoreFactory.eINSTANCE.createEReference();
				eRef.setName("ref");
				eRef.setEType(eClass);
				eRef.setLowerBound(0);
				eRef.setUpperBound(-1);
				eRef.setEOpposite(eRef);
				eRef.setContainment(false);
				eRef.setOrdered(true);
				eRef.setUnique(true);
				eClass.getEStructuralFeatures().add(eRef);
			}
			ePackage.getEClassifiers().add(eClass);
		}
		this.registerEPackages(ePackage);
	}

	private void createEPackageMultiplicityOneContainmentNoOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EReference eReference = EcoreFactory.eINSTANCE.createEReference();
		eReference.setName("child");
		eReference.setEType(eOtherClass);
		eReference.setLowerBound(0);
		eReference.setUpperBound(1);
		eReference.setContainment(true);
		eClass.getEStructuralFeatures().add(eReference);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageMultiplicityOneContainmentWithOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EReference eRefChild = EcoreFactory.eINSTANCE.createEReference();
		eRefChild.setName("child");
		eRefChild.setEType(eOtherClass);
		eRefChild.setLowerBound(0);
		eRefChild.setUpperBound(1);
		eRefChild.setContainment(true);
		EReference eRefParent = EcoreFactory.eINSTANCE.createEReference();
		eRefParent.setName("parent");
		eRefParent.setLowerBound(0);
		eRefParent.setUpperBound(1);
		eRefParent.setEType(eClass);
		eRefParent.setEOpposite(eRefChild);
		eRefChild.setEOpposite(eRefParent);
		eClass.getEStructuralFeatures().add(eRefChild);
		eOtherClass.getEStructuralFeatures().add(eRefParent);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageMultiplicityManyContainmentNoOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EAttribute eAttribute = EcoreFactory.eINSTANCE.createEAttribute();
		eAttribute.setName("MyAttribute");
		eAttribute.setEType(EcorePackage.Literals.ESTRING);
		eAttribute.setLowerBound(0);
		eAttribute.setUpperBound(1);
		eClass.getEStructuralFeatures().add(eAttribute);
		EReference eReference = EcoreFactory.eINSTANCE.createEReference();
		eReference.setName("children");
		eReference.setEType(eOtherClass);
		eReference.setLowerBound(0);
		eReference.setUpperBound(-1);
		eReference.setContainment(true);
		eClass.getEStructuralFeatures().add(eReference);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageMultiplicityManyContainmentWithMultiplicityOneOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		ePackage.setName("MyPackage");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		EReference eRefChildren = EcoreFactory.eINSTANCE.createEReference();
		eRefChildren.setName("children");
		eRefChildren.setEType(eOtherClass);
		eRefChildren.setLowerBound(0);
		eRefChildren.setUpperBound(-1);
		eRefChildren.setContainment(true);
		eClass.getEStructuralFeatures().add(eRefChildren);
		EReference eRefParent = EcoreFactory.eINSTANCE.createEReference();
		eRefParent.setName("parent");
		eRefParent.setLowerBound(0);
		eRefParent.setUpperBound(1);
		eRefParent.setEType(eClass);
		eRefParent.setEOpposite(eRefChildren);
		eRefChildren.setEOpposite(eRefParent);
		eOtherClass.getEStructuralFeatures().add(eRefParent);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		this.registerEPackages(ePackage);
	}

	private void createEPackageInheritedMultiplicityManyContainmentWithNonInheritedMultiplicityOneOpposite() {
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		ePackage.setName("MyPackage");

		EClass ecIContainer = EcoreFactory.eINSTANCE.createEClass();
		ecIContainer.setName("IContainer");

		EClass ecContainer = EcoreFactory.eINSTANCE.createEClass();
		ecContainer.setName("Container");
		ecContainer.getESuperTypes().add(ecIContainer);

		EClass ecIElement = EcoreFactory.eINSTANCE.createEClass();
		ecIElement.setName("IElement");

		EClass ecElement = EcoreFactory.eINSTANCE.createEClass();
		ecElement.setName("Element");
		ecElement.getESuperTypes().add(ecIElement);

		EReference erElements = EcoreFactory.eINSTANCE.createEReference();
		erElements.setName("elements");
		erElements.setEType(ecIElement);
		erElements.setLowerBound(0);
		erElements.setUpperBound(-1);
		erElements.setContainment(true);
		ecIContainer.getEStructuralFeatures().add(erElements);

		EAttribute eaA = EcoreFactory.eINSTANCE.createEAttribute();
		eaA.setName("a");
		eaA.setEType(EcorePackage.Literals.ESTRING);
		eaA.setLowerBound(0);
		eaA.setUpperBound(1);
		ecIContainer.getEStructuralFeatures().add(eaA);

		EAttribute eaB = EcoreFactory.eINSTANCE.createEAttribute();
		eaB.setName("b");
		eaB.setEType(EcorePackage.Literals.ESTRING);
		eaB.setLowerBound(0);
		eaB.setUpperBound(1);
		ecIContainer.getEStructuralFeatures().add(eaB);

		EReference erContainer = EcoreFactory.eINSTANCE.createEReference();
		erContainer.setName("container");
		erContainer.setLowerBound(0);
		erContainer.setUpperBound(1);
		erContainer.setEType(ecIContainer);
		erContainer.setEOpposite(erElements);
		erElements.setEOpposite(erContainer);
		ecIElement.getEStructuralFeatures().add(erContainer);
		ePackage.getEClassifiers().add(ecIContainer);
		ePackage.getEClassifiers().add(ecContainer);
		ePackage.getEClassifiers().add(ecIElement);
		ePackage.getEClassifiers().add(ecElement);

		this.registerEPackages(ePackage);
	}

	private void createGrabatsFragmentsEPackage() {
		// the following is a fragment of the 'JDTAST.ecore' model (from GRABATS).
		EPackage ePackage = this.createNewEPackage("MyEPackage", "http://www.example.com/model", "model");
		{
			EClass ecIJavaElement = EcoreFactory.eINSTANCE.createEClass();
			ecIJavaElement.setName("IJavaElement");
			ecIJavaElement.setAbstract(true);
			{
				EAttribute eaElementName = EcoreFactory.eINSTANCE.createEAttribute();
				eaElementName.setName("elementName");
				eaElementName.setEType(EcorePackage.Literals.ESTRING);
				eaElementName.setLowerBound(1);
				eaElementName.setOrdered(false);
				eaElementName.setUnique(false);
				ecIJavaElement.getEStructuralFeatures().add(eaElementName);
			}
			ePackage.getEClassifiers().add(ecIJavaElement);

			EClass ecPhysicalElement = EcoreFactory.eINSTANCE.createEClass();
			ecPhysicalElement.setName("PhysicalElement");
			ecPhysicalElement.setAbstract(true);
			{
				EAttribute eaPath = EcoreFactory.eINSTANCE.createEAttribute();
				eaPath.setName("path");
				eaPath.setEType(EcorePackage.Literals.ESTRING);
				eaPath.setLowerBound(1);
				eaPath.setOrdered(false);
				eaPath.setUnique(false);
				ecPhysicalElement.getEStructuralFeatures().add(eaPath);

				EAttribute eaIsReadOnly = EcoreFactory.eINSTANCE.createEAttribute();
				eaIsReadOnly.setName("isReadOnly");
				eaIsReadOnly.setEType(EcorePackage.Literals.EBOOLEAN);
				eaIsReadOnly.setOrdered(false);
				eaIsReadOnly.setUnique(false);
				eaIsReadOnly.setLowerBound(1);
				ecPhysicalElement.getEStructuralFeatures().add(eaIsReadOnly);
			}
			ePackage.getEClassifiers().add(ecPhysicalElement);

			EClass ecIPackageFragment = EcoreFactory.eINSTANCE.createEClass();
			ecIPackageFragment.setName("IPackageFragment");
			ecIPackageFragment.getESuperTypes().add(ecIJavaElement);
			ecIPackageFragment.getESuperTypes().add(ecPhysicalElement);
			{
				EAttribute eaIsDefaultPackage = EcoreFactory.eINSTANCE.createEAttribute();
				eaIsDefaultPackage.setName("isDefaultPackage");
				eaIsDefaultPackage.setEType(EcorePackage.Literals.EBOOLEAN);
				eaIsDefaultPackage.setLowerBound(1);
				eaIsDefaultPackage.setOrdered(false);
				eaIsDefaultPackage.setUnique(false);
				ecIPackageFragment.getEStructuralFeatures().add(eaIsDefaultPackage);
			}
			ePackage.getEClassifiers().add(ecIPackageFragment);

			EClass ecIPackageFragmentRoot = EcoreFactory.eINSTANCE.createEClass();
			ecIPackageFragmentRoot.setName("IPackageFragmentRoot");
			ecIPackageFragmentRoot.setAbstract(true);
			ecIPackageFragmentRoot.getESuperTypes().add(ecPhysicalElement);
			ecIPackageFragmentRoot.getESuperTypes().add(ecIJavaElement);
			{
				// no additional features
			}
			ePackage.getEClassifiers().add(ecIPackageFragmentRoot);

			// add some bidirectional references
			EReference erPackageFragments = EcoreFactory.eINSTANCE.createEReference();
			erPackageFragments.setName("packageFragments");
			erPackageFragments.setEType(ecIPackageFragment);
			erPackageFragments.setOrdered(false);
			erPackageFragments.setUpperBound(-1);
			erPackageFragments.setContainment(true);
			ecIPackageFragmentRoot.getEStructuralFeatures().add(erPackageFragments);

			EReference erPackageFragmentRoot = EcoreFactory.eINSTANCE.createEReference();
			erPackageFragmentRoot.setName("packageFragmentRoot");
			erPackageFragmentRoot.setEType(ecIPackageFragmentRoot);
			erPackageFragmentRoot.setOrdered(false);
			erPackageFragmentRoot.setLowerBound(1);
			ecIPackageFragment.getEStructuralFeatures().add(erPackageFragmentRoot);

			// these references are opposites of each other
			erPackageFragmentRoot.setEOpposite(erPackageFragments);
			erPackageFragments.setEOpposite(erPackageFragmentRoot);

			EClass ecBinaryPackageFragmentRoot = EcoreFactory.eINSTANCE.createEClass();
			ecBinaryPackageFragmentRoot.setName("BinaryPackageFragmentRoot");
			ecBinaryPackageFragmentRoot.getESuperTypes().add(ecIPackageFragmentRoot);
			{
				// no additional features
			}
			ePackage.getEClassifiers().add(ecBinaryPackageFragmentRoot);
		}
		this.registerEPackages(ePackage);
	}

}
