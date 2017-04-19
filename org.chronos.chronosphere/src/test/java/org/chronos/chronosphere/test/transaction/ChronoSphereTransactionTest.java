package org.chronos.chronosphere.test.transaction;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.impl.ChronoEFactory;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.chronosphere.testutils.EMFTestUtils;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;

@Category(IntegrationTest.class)
public class ChronoSphereTransactionTest extends AllChronoSphereBackendsTest {

	@Test
	public void canOpenAndCloseTransaction() {
		ChronoSphere sphere = this.getChronoSphere();
		ChronoSphereTransaction tx = sphere.tx();
		assertNotNull(tx);
		assertTrue(tx.isOpen());
		assertFalse(tx.isClosed());
		tx.close();
		assertFalse(tx.isOpen());
		assertTrue(tx.isClosed());
	}

	@Test
	public void canConfigureEPackage() {
		ChronoSphere sphere = this.getChronoSphere();
		// create a simple dummy EPackage
		EPackage ePackage = this.createSimpleEPackage();
		// register the EPackage at ChronoSphere
		sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);
		// the registration should have replaced our EFactory
		assertTrue(ePackage.getEFactoryInstance() instanceof ChronoEFactory);
		// check that the EPackage is registered by opening a new transaction and checking the registry
		ChronoSphereTransaction tx = sphere.tx();
		EPackage storedPackage = tx.getEPackageByNsURI("http://com.example.model.MyEPackage");
		assertNotNull(storedPackage);
		tx.close();
	}

	@Test
	public void canConfigureEPackageWithNestedSubpackages() {
		ChronoSphere sphere = this.getChronoSphere();
		// create the EPackage that contains subpackages
		EPackage ePackage = this.createEPackageWithNestedEPackages();
		// register the EPackage at ChronoSphere
		sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);
		// the registration should have replaced our EFactory
		assertTrue(ePackage.getEFactoryInstance() instanceof ChronoEFactory);
		// check that the EPackage is registered by opening a new transaction and checking the registry
		ChronoSphereTransaction tx = sphere.tx();
		EPackage storedPackage = tx.getEPackageByNsURI("http://com.example.model.MyEPackage");
		assertNotNull(storedPackage);
		tx.close();
	}

	@Test
	public void canAttachAndLoadSimpleEObject() {
		ChronoSphere sphere = this.getChronoSphere();
		// create a simple dummy EPackage
		EPackage ePackage = this.createSimpleEPackage();
		// register the EPackage at ChronoSphere
		sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

		String eObjectID = null;

		// open a transaction, and write an EObject
		{
			ChronoSphereTransaction tx = sphere.tx();
			// fetch the EClass from the package
			EClass eClass = (EClass) tx.getEPackageByNsURI("http://com.example.model.MyEPackage")
					.getEClassifier("MyEClass");
			assertNotNull(eClass);
			// fetch the attribute
			EAttribute eaName = (EAttribute) eClass.getEStructuralFeature("name");
			assertNotNull(eaName);
			// create an EObject
			EObject eObject = EcoreUtil.create(eClass);
			assertTrue(eObject instanceof ChronoEObject);
			eObject.eSet(eaName, "MyEObject");
			eObjectID = ((ChronoEObject) eObject).getId();
			// make sure that the EObject has an ID
			assertNotNull(eObjectID);
			// attach the EObject to the transaction
			tx.attach(eObject);
			// assert that the attachment was successful
			assertTrue(((ChronoEObjectInternal) eObject).isAttached());
			// commit the transaction
			tx.commit();
		}

		// now, when we open a transaction, we should be able to retrieve the EObject by ID
		{
			ChronoSphereTransaction tx = sphere.tx();
			// fetch the EClass from the package
			EClass eClass = (EClass) tx.getEPackageByNsURI("http://com.example.model.MyEPackage")
					.getEClassifier("MyEClass");
			assertNotNull(eClass);
			// fetch the attribute
			EAttribute eaName = (EAttribute) eClass.getEStructuralFeature("name");
			assertNotNull(eaName);
			assertTrue(tx.getTimestamp() > 0);
			EObject eObject = tx.getEObjectById(eObjectID);
			assertNotNull(eObject);
			assertEquals(eClass, eObject.eClass());
			assertEquals("MyEObject", eObject.eGet(eaName));
		}

	}

	@Test
	@SuppressWarnings("unchecked")
	public void canCreateCrossReferencesBetweenAttachedEObjects() {
		ChronoSphere sphere = this.getChronoSphere();
		// create an EPackage
		EPackage ePackage = this.createEPackageWithNestedEPackages();
		// register the EPackage at ChronoSphere
		sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

		String eObject1ID = null;
		String eObject2ID = null;
		String eObject3ID = null;

		// open a transaction, and write the three EObjects
		{
			ChronoSphereTransaction tx = sphere.tx();
			// fetch the EClasses from the package
			EClass myEClass = EMFTestUtils
					.getEClassRecursive(tx.getEPackageByNsURI("http://com.example.model.MyEPackage"), "MyEClass");
			EClass yourEClass = EMFTestUtils
					.getEClassRecursive(tx.getEPackageByNsURI("http://com.example.model.MyEPackage"), "YourEClass");
			assertNotNull(myEClass);
			assertNotNull(yourEClass);
			// fetch the attribute
			EAttribute eaName = (EAttribute) myEClass.getEStructuralFeature("name");
			assertNotNull(eaName);
			// create an EObject
			EObject eObject1 = EcoreUtil.create(myEClass);
			assertTrue(eObject1 instanceof ChronoEObject);
			eObject1.eSet(eaName, "EObject1");
			eObject1ID = ((ChronoEObject) eObject1).getId();
			// make sure that the EObject has an ID
			assertNotNull(eObject1ID);
			// attach the EObject to the transaction
			tx.attach(eObject1);
			// assert that the attachment was successful
			assertTrue(((ChronoEObjectInternal) eObject1).isAttached());

			// create another EObject
			EObject eObject2 = EcoreUtil.create(yourEClass);
			assertTrue(eObject2 instanceof ChronoEObject);
			eObject2.eSet(eaName, "EObject2");
			eObject2ID = ((ChronoEObject) eObject2).getId();
			// make sure that the EObject has an ID
			assertNotNull(eObject2ID);
			// attach the EObject to the transaction
			tx.attach(eObject2);
			// assert that the attachment was successful
			assertTrue(((ChronoEObjectInternal) eObject2).isAttached());

			// create another EObject
			EObject eObject3 = EcoreUtil.create(yourEClass);
			assertTrue(eObject3 instanceof ChronoEObject);
			eObject3.eSet(eaName, "EObject3");
			eObject3ID = ((ChronoEObject) eObject3).getId();
			// make sure that the EObject has an ID
			assertNotNull(eObject3ID);
			// attach the EObject to the transaction
			tx.attach(eObject3);
			// assert that the attachment was successful
			assertTrue(((ChronoEObjectInternal) eObject3).isAttached());

			// get the EReference
			EReference eReference = (EReference) myEClass.getEStructuralFeature("knows");
			assertNotNull(eReference);
			assertTrue(eReference.isMany());
			// connect EObject1->EObject2, EObject1->EObject3
			((List<EObject>) eObject1.eGet(eReference)).add(eObject2);
			((List<EObject>) eObject1.eGet(eReference)).add(eObject3);

			// commit the transaction
			tx.commit();
		}

		// open another transaction and check that everything's there
		{
			ChronoSphereTransaction tx = sphere.tx();
			// get the metamodel stuff
			EClass myEClass = EMFTestUtils
					.getEClassRecursive(tx.getEPackageByNsURI("http://com.example.model.MyEPackage"), "MyEClass");
			EClass yourEClass = EMFTestUtils
					.getEClassRecursive(tx.getEPackageByNsURI("http://com.example.model.MyEPackage"), "YourEClass");
			EReference eReference = (EReference) myEClass.getEStructuralFeature("knows");
			// load EObject 1
			ChronoEObject eObject1 = tx.getEObjectById(eObject1ID);
			assertNotNull(eObject1);
			assertEquals(eObject1ID, eObject1.getId());
			// try to navigate to EObject2 and EObject3
			List<ChronoEObject> knownEObjects = (List<ChronoEObject>) eObject1.eGet(eReference);
			assertEquals(2, knownEObjects.size());
			ChronoEObject eObject2 = knownEObjects.get(0);
			ChronoEObject eObject3 = knownEObjects.get(1);

			// the first eObject should be eObject2
			assertEquals(eObject2ID, eObject2.getId());
			// ... and it should be an instance of "YourEClass"
			assertTrue(yourEClass.isInstance(eObject2));
			// the second eObject should be eObject3
			assertEquals(eObject3ID, eObject3.getId());
			// ... and it should be an instance of "YourEClass"
			assertTrue(yourEClass.isInstance(eObject3));
		}
	}

	@Test
	public void canCreateMultiplicityManyNonUniqueOrderedCrossReferences() {
		ChronoSphere sphere = this.getChronoSphere();
		// prepare the EPackage
		EPackage ePackage = this.createEPackageWithNonUniqueMultiplicityManyOrderedCrossReference();
		sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);
		// open a transaction and create some EObject instances based on the EPackage
		try (ChronoSphereTransaction tx = sphere.tx()) {
			ePackage = tx.getEPackageByNsURI("http://com.example.model.MyEPackage");
			EClass eClass = (EClass) ePackage.getEClassifier("MyClass");
			assertNotNull(eClass);
			EReference eRef = (EReference) eClass.getEStructuralFeature("ref");
			assertNotNull(eRef);
			EAttribute name = (EAttribute) eClass.getEStructuralFeature("Name");
			assertNotNull(name);
			EObject eObj1 = tx.createAndAttach(eClass);
			eObj1.eSet(name, "EObj1");
			EObject eObj2 = tx.createAndAttach(eClass);
			eObj2.eSet(name, "EObj2");
			EObject eObj3 = tx.createAndAttach(eClass);
			eObj3.eSet(name, "EObj3");
			EObject eObj4 = tx.createAndAttach(eClass);
			eObj4.eSet(name, "EObj4");
			EList<EObject> targets = EMFUtils.eGetMany(eObj1, eRef);
			targets.add(eObj2);
			targets.add(eObj3);
			targets.add(eObj3);
			targets.add(eObj2);
			targets.add(eObj4);

			EList<EObject> targets2 = EMFUtils.eGetMany(eObj1, eRef);
			assertEquals(targets, targets2);
			assertEquals(eObj2, targets2.get(0));
			assertEquals(eObj3, targets2.get(1));
			assertEquals(eObj3, targets2.get(2));
			assertEquals(eObj2, targets2.get(3));
			assertEquals(eObj4, targets2.get(4));
			tx.commit();
		}
		// assert that the references are okay via standard Ecore methods
		try (ChronoSphereTransaction tx = sphere.tx()) {
			ePackage = tx.getEPackageByNsURI("http://com.example.model.MyEPackage");
			EClass eClass = (EClass) ePackage.getEClassifier("MyClass");
			assertNotNull(eClass);
			EReference eRef = (EReference) eClass.getEStructuralFeature("ref");
			assertNotNull(eRef);
			EAttribute name = (EAttribute) eClass.getEStructuralFeature("Name");
			assertNotNull(name);
			Set<EObject> queryResult = tx.find().startingFromAllEObjects().has(name, "EObj1").toSet();
			EObject eObj1 = Iterables.getOnlyElement(queryResult);
			assertNotNull(eObj1);
			EList<EObject> targets = EMFUtils.eGetMany(eObj1, eRef);
			assertEquals("EObj2", targets.get(0).eGet(name));
			assertEquals("EObj3", targets.get(1).eGet(name));
			assertEquals("EObj3", targets.get(2).eGet(name));
			assertEquals("EObj2", targets.get(3).eGet(name));
			assertEquals("EObj4", targets.get(4).eGet(name));
		}
		// assert that the references are okay via EQuery
		try (ChronoSphereTransaction tx = sphere.tx()) {
			ePackage = tx.getEPackageByNsURI("http://com.example.model.MyEPackage");
			EClass eClass = (EClass) ePackage.getEClassifier("MyClass");
			assertNotNull(eClass);
			EReference eRef = (EReference) eClass.getEStructuralFeature("ref");
			assertNotNull(eRef);
			EAttribute name = (EAttribute) eClass.getEStructuralFeature("Name");
			assertNotNull(name);
			List<EObject> targets = tx.find().startingFromAllEObjects().has(name, "EObj1").eGet(eRef).toList();
			assertEquals("EObj2", targets.get(0).eGet(name));
			assertEquals("EObj3", targets.get(1).eGet(name));
			assertEquals("EObj3", targets.get(2).eGet(name));
			assertEquals("EObj2", targets.get(3).eGet(name));
			assertEquals("EObj4", targets.get(4).eGet(name));
		}
	}

	@Test
	public void canWorkWithGrabatsFragmentModel() {
		// the following test uses a fragment of the 'JDTAST.ecore' model (from GRABATS).
		ChronoSphere sphere = this.getChronoSphere();

		// create a model and attach it
		String packageRootID = null;
		String frag1ID = null;
		String frag2ID = null;
		String frag3ID = null;
		{
			// prepare the EPackage
			EPackage ePackage = this.createEPackageForGrabatsFragmentTest();
			sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

			EClass ecIPackageFragment = (EClass) ePackage.getEClassifier("IPackageFragment");
			assertNotNull(ecIPackageFragment);
			EClass ecBinaryPackageFragmentRoot = (EClass) ePackage.getEClassifier("BinaryPackageFragmentRoot");
			assertNotNull(ecBinaryPackageFragmentRoot);

			EReference erPackageFragments = (EReference) ecBinaryPackageFragmentRoot
					.getEStructuralFeature("packageFragments");
			assertNotNull(erPackageFragments);

			EReference erPackageFragmentRoot = (EReference) ecIPackageFragment
					.getEStructuralFeature("packageFragmentRoot");
			assertNotNull(erPackageFragmentRoot);

			EAttribute eaElementName = (EAttribute) ecBinaryPackageFragmentRoot.getEStructuralFeature("elementName");
			assertNotNull(eaElementName);

			EAttribute eaPath = (EAttribute) ecIPackageFragment.getEStructuralFeature("path");
			assertNotNull(eaPath);

			EAttribute eaIsReadOnly = (EAttribute) ecIPackageFragment.getEStructuralFeature("isReadOnly");
			assertNotNull(eaIsReadOnly);
			EObject packageRoot = EcoreUtil.create(ecBinaryPackageFragmentRoot);
			packageRootID = ((ChronoEObject) packageRoot).getId();
			packageRoot.eSet(eaElementName, "root");
			packageRoot.eSet(eaPath, "/");
			packageRoot.eSet(eaIsReadOnly, true);

			EObject frag1 = EcoreUtil.create(ecIPackageFragment);
			frag1ID = ((ChronoEObject) frag1).getId();
			assertEquals(ecIPackageFragment, frag1.eClass());
			frag1.eSet(eaElementName, "Frag1");
			frag1.eSet(eaIsReadOnly, true);
			frag1.eSet(eaPath, ".frag1");
			EMFUtils.eGetMany(packageRoot, erPackageFragments).add(frag1);
			// frag1.eSet(erPackageFragmentRoot, packageRoot);

			EObject frag2 = EcoreUtil.create(ecIPackageFragment);
			frag2ID = ((ChronoEObject) frag2).getId();
			assertEquals(ecIPackageFragment, frag2.eClass());
			frag2.eSet(eaElementName, "Frag2");
			frag2.eSet(eaIsReadOnly, true);
			frag2.eSet(eaPath, ".frag2");
			frag2.eSet(erPackageFragmentRoot, packageRoot);

			EObject frag3 = EcoreUtil.create(ecIPackageFragment);
			frag3ID = ((ChronoEObject) frag3).getId();
			assertEquals(ecIPackageFragment, frag3.eClass());
			frag3.eSet(eaElementName, "Frag3");
			frag3.eSet(eaIsReadOnly, true);
			frag3.eSet(eaPath, ".frag3");
			frag3.eSet(erPackageFragmentRoot, packageRoot);

			assertEquals(packageRoot, frag1.eGet(erPackageFragmentRoot));
			assertEquals(packageRoot, frag1.eContainer());

			sphere.batchInsertModelData(packageRoot);
		}

		try (ChronoSphereTransaction tx = sphere.tx()) {
			// extract EPackage data
			EPackage ePackage = tx.getEPackageByNsURI("http://com.example.model.MyEPackage");
			assertNotNull(ePackage);
			EClass ecIPackageFragment = (EClass) ePackage.getEClassifier("IPackageFragment");
			assertNotNull(ecIPackageFragment);
			EClass ecBinaryPackageFragmentRoot = (EClass) ePackage.getEClassifier("BinaryPackageFragmentRoot");
			assertNotNull(ecBinaryPackageFragmentRoot);

			EReference erPackageFragments = (EReference) ecBinaryPackageFragmentRoot
					.getEStructuralFeature("packageFragments");
			assertNotNull(erPackageFragments);

			EReference erPackageFragmentRoot = (EReference) ecIPackageFragment
					.getEStructuralFeature("packageFragmentRoot");
			assertNotNull(erPackageFragmentRoot);

			EAttribute eaElementName = (EAttribute) ecBinaryPackageFragmentRoot.getEStructuralFeature("elementName");
			assertNotNull(eaElementName);

			EAttribute eaPath = (EAttribute) ecIPackageFragment.getEStructuralFeature("path");
			assertNotNull(eaPath);

			EAttribute eaIsReadOnly = (EAttribute) ecIPackageFragment.getEStructuralFeature("isReadOnly");
			assertNotNull(eaIsReadOnly);

			EObject packageRoot = tx.getEObjectById(packageRootID);
			assertNotNull(packageRoot);
			EObject frag1 = tx.getEObjectById(frag1ID);
			assertNotNull(frag1);
			assertEquals(ecIPackageFragment, frag1.eClass());
			EObject frag2 = tx.getEObjectById(frag2ID);
			assertNotNull(frag2);
			assertEquals(ecIPackageFragment, frag2.eClass());
			EObject frag3 = tx.getEObjectById(frag3ID);
			assertNotNull(frag3);
			assertEquals(ecIPackageFragment, frag3.eClass());

			assertEquals(packageRoot, frag1.eGet(erPackageFragmentRoot));
			assertEquals(packageRoot, frag1.eContainer());
			assertEquals(packageRoot, frag2.eContainer());
			assertEquals(packageRoot, frag3.eContainer());

			assertEquals(3, tx.find().startingFromAllEObjects().named("fragments").eGet("packageFragmentRoot")
					.back("fragments").count());
		}
	}

	// =====================================================================================================================
	// EPACKAGES
	// =====================================================================================================================

	private EPackage createSimpleEPackage() {
		EPackage ePackage = EcoreFactory.eINSTANCE.createEPackage();
		ePackage.setName("MyEPackage");
		ePackage.setNsURI("http://com.example.model.MyEPackage");
		ePackage.setNsPrefix("com.example");
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
		EAttribute eaName = EcoreFactory.eINSTANCE.createEAttribute();
		eaName.setName("name");
		eaName.setLowerBound(0);
		eaName.setUpperBound(1);
		eaName.setEType(EcorePackage.Literals.ESTRING);
		eClass.getEStructuralFeatures().add(eaName);
		ePackage.getEClassifiers().add(eClass);
		ePackage.getEClassifiers().add(eOtherClass);
		return ePackage;
	}

	private EPackage createEPackageWithNestedEPackages() {
		EPackage rootEPackage = EcoreFactory.eINSTANCE.createEPackage();
		rootEPackage.setName("MyEPackage");
		rootEPackage.setNsURI("http://com.example.model.MyEPackage");
		rootEPackage.setNsPrefix("com.example");
		EPackage sub1 = EcoreFactory.eINSTANCE.createEPackage();
		sub1.setName("sub1");
		sub1.setNsURI("http://com.example.model.MyEPackage.sub1");
		sub1.setNsPrefix("com.example.sub1");
		rootEPackage.getESubpackages().add(sub1);
		EPackage sub2 = EcoreFactory.eINSTANCE.createEPackage();
		sub2.setName("sub2");
		sub2.setNsURI("http://com.example.model.MyEPackage.sub2");
		sub2.setNsPrefix("com.example.sub2");
		rootEPackage.getESubpackages().add(sub2);
		EPackage sub1a = EcoreFactory.eINSTANCE.createEPackage();
		sub1a.setName("sub1a");
		sub1a.setNsURI("http://com.example.model.MyEPackage.sub1.sub1a");
		sub1a.setNsPrefix("com.example.sub1.sub1a");
		sub1.getESubpackages().add(sub1a);
		EPackage sub1b = EcoreFactory.eINSTANCE.createEPackage();
		sub1b.setName("sub1b");
		sub1b.setNsURI("http://com.example.model.MyEPackage.sub1.sub1b");
		sub1b.setNsPrefix("com.example.sub1.sub1b");
		sub1.getESubpackages().add(sub1b);

		EClass eBaseClass = EcoreFactory.eINSTANCE.createEClass();
		eBaseClass.setName("BaseClass");

		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		eClass.getESuperTypes().add(eBaseClass);
		EClass eOtherClass = EcoreFactory.eINSTANCE.createEClass();
		eOtherClass.setName("YourEClass");
		eOtherClass.getESuperTypes().add(eBaseClass);
		EReference eReference = EcoreFactory.eINSTANCE.createEReference();
		eReference.setName("knows");
		eReference.setEType(eOtherClass);
		eReference.setLowerBound(0);
		eReference.setUpperBound(-1);
		eReference.setContainment(false);
		eClass.getEStructuralFeatures().add(eReference);
		EAttribute eaName = EcoreFactory.eINSTANCE.createEAttribute();
		eaName.setName("name");
		eaName.setLowerBound(0);
		eaName.setUpperBound(1);
		eaName.setEType(EcorePackage.Literals.ESTRING);
		eBaseClass.getEStructuralFeatures().add(eaName);
		sub2.getEClassifiers().add(eBaseClass);
		sub1a.getEClassifiers().add(eClass);
		sub1b.getEClassifiers().add(eOtherClass);
		return rootEPackage;
	}

	private EPackage createEPackageWithNonUniqueMultiplicityManyOrderedCrossReference() {
		EPackage ePackage = EcoreFactory.eINSTANCE.createEPackage();
		ePackage.setName("MyEPackage");
		ePackage.setNsURI("http://com.example.model.MyEPackage");
		ePackage.setNsPrefix("com.example");
		{
			EClass eClass = EcoreFactory.eINSTANCE.createEClass();
			eClass.setName("MyClass");
			{
				{
					EReference eRef = EcoreFactory.eINSTANCE.createEReference();
					eRef.setName("ref");
					// multiplicity-many
					eRef.setLowerBound(0);
					eRef.setUpperBound(-1);
					// non-unique
					eRef.setUnique(false);
					// ordered
					eRef.setOrdered(true);
					// cross-reference
					eRef.setContainment(false);
					eRef.setEType(eClass);
					eClass.getEStructuralFeatures().add(eRef);
				}
				{
					EAttribute eaName = EcoreFactory.eINSTANCE.createEAttribute();
					eaName.setName("Name");
					eaName.setLowerBound(0);
					eaName.setUpperBound(1);
					eaName.setEType(EcorePackage.Literals.ESTRING);
					eClass.getEStructuralFeatures().add(eaName);
				}
			}
			ePackage.getEClassifiers().add(eClass);
		}
		return ePackage;
	}

	private EPackage createEPackageForGrabatsFragmentTest() {
		// the following is a fragment of the 'JDTAST.ecore' model (from GRABATS).
		EPackage ePackage = EcoreFactory.eINSTANCE.createEPackage();
		ePackage.setName("MyEPackage");
		ePackage.setNsURI("http://com.example.model.MyEPackage");
		ePackage.setNsPrefix("com.example");
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
			return ePackage;
		}
	}
}
