package org.chronos.chronosphere.test.emf;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.test.base.ChronoSphereUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.EcorePackage;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterables;

@Category(UnitTest.class)
public class EMFUtilsTest extends ChronoSphereUnitTest {

	@Test
	public void canConvertBetweenEPackageAndXMI() {
		EPackage ePackage = EcoreFactory.eINSTANCE.createEPackage();
		ePackage.setName("MyEPackage");
		EClass eClass = EcoreFactory.eINSTANCE.createEClass();
		eClass.setName("MyEClass");
		EAttribute eAttribute = EcoreFactory.eINSTANCE.createEAttribute();
		eAttribute.setName("MyAttribute");
		eAttribute.setEType(EcorePackage.Literals.ESTRING);
		eAttribute.setLowerBound(0);
		eAttribute.setUpperBound(1);
		eClass.getEStructuralFeatures().add(eAttribute);
		EReference eReference = EcoreFactory.eINSTANCE.createEReference();
		eReference.setName("self");
		eReference.setEType(eClass);
		eReference.setUpperBound(-1);
		eReference.setLowerBound(0);
		eClass.getEStructuralFeatures().add(eReference);
		ePackage.getEClassifiers().add(eClass);

		String xmi = EMFUtils.writeEPackageToXMI(ePackage);
		assertNotNull(xmi);
		assertFalse(xmi.trim().isEmpty());

		// print it for debugging purposes
		System.out.println(xmi);

		// deserialize
		EPackage ePackage2 = EMFUtils.readEPackageFromXMI(xmi);
		assertNotNull(ePackage2);
		assertEquals("MyEPackage", ePackage2.getName());
		EClass eClass2 = (EClass) ePackage2.getEClassifier("MyEClass");
		assertNotNull(eClass2);
		assertEquals("MyEClass", eClass2.getName());
		EAttribute eAttribute2 = Iterables.getOnlyElement(eClass2.getEAttributes());
		assertNotNull(eAttribute2);
		assertEquals("MyAttribute", eAttribute2.getName());
		assertEquals(0, eAttribute2.getLowerBound());
		assertEquals(1, eAttribute2.getUpperBound());
		assertEquals(EcorePackage.Literals.ESTRING, eAttribute2.getEType());
		EReference eReference2 = Iterables.getOnlyElement(eClass2.getEReferences());
		assertNotNull(eReference2);
		assertEquals("self", eReference2.getName());
		assertEquals(eClass2, eReference2.getEReferenceType());
		assertEquals(0, eReference2.getLowerBound());
		assertEquals(-1, eReference2.getUpperBound());

	}

	@Test
	public void getEPackageByQualifiedName() {
		Collection<EPackage> ePackages = loadSocialNetworkPackages();
		EPackage socialNetworkPackage = EMFUtils.getEPackageByQualifiedName(ePackages, "socialnetwork");
		assertNotNull(socialNetworkPackage);
		assertEquals("socialnetwork", socialNetworkPackage.getName());
		EPackage activityPackage = EMFUtils.getEPackageByQualifiedName(ePackages, "socialnetwork::activity");
		assertNotNull(activityPackage);
		assertEquals("activity", activityPackage.getName());
	}

	@Test
	public void getEClassByQualifiedName() {
		Collection<EPackage> ePackages = loadSocialNetworkPackages();
		EClass ecPerson = EMFUtils.getEClassByQualifiedName(ePackages, "socialnetwork::user::Person");
		assertNotNull(ecPerson);
		assertEquals("Person", ecPerson.getName());
	}

	@Test
	public void getFeatureByQualifiedName() {
		Collection<EPackage> ePackages = loadSocialNetworkPackages();
		EStructuralFeature eaFirstName = EMFUtils.getFeatureByQualifiedName(ePackages,
				"socialnetwork::user::Person#firstName");
		assertNotNull(eaFirstName);
		assertEquals("firstName", eaFirstName.getName());
	}

	@Test
	public void getEAttributeByQualifiedName() {
		Collection<EPackage> ePackages = loadSocialNetworkPackages();
		EAttribute eaFirstName = EMFUtils.getEAttributeByQualifiedName(ePackages,
				"socialnetwork::user::Person#firstName");
		assertNotNull(eaFirstName);
		assertEquals("firstName", eaFirstName.getName());
	}

	@Test
	public void getEReferenceByQualifiedName() {
		Collection<EPackage> ePackages = loadSocialNetworkPackages();
		EReference eaFriends = EMFUtils.getEReferenceByQualifiedName(ePackages, "socialnetwork::user::Person#friends");
		assertNotNull(eaFriends);
		assertEquals("friends", eaFriends.getName());
	}

	@Test
	public void getEPackageBySimpleName() {
		Collection<EPackage> ePackages = loadSocialNetworkPackages();
		EPackage socialNetworkPackage = EMFUtils.getEPackageBySimpleName(ePackages, "socialnetwork");
		assertNotNull(socialNetworkPackage);
		assertEquals("socialnetwork", socialNetworkPackage.getName());
		EPackage activityPackage = EMFUtils.getEPackageBySimpleName(ePackages, "activity");
		assertNotNull(activityPackage);
		assertEquals("activity", activityPackage.getName());
	}

	@Test
	public void getEClassBySimpleName() {
		Collection<EPackage> ePackages = loadSocialNetworkPackages();
		EClass ecPerson = EMFUtils.getEClassBySimpleName(ePackages, "Person");
		assertNotNull(ecPerson);
		assertEquals("Person", ecPerson.getName());
	}

	private static Collection<EPackage> loadSocialNetworkPackages() {
		try {
			InputStream inputStream = EMFUtilsTest.class.getClassLoader()
					.getResourceAsStream("org/chronos/chronosphere/test/metamodels/socialnetwork.ecore");
			String xmiContents = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			return EMFUtils.readEPackagesFromXMI(xmiContents);
		} catch (Exception e) {
			throw new RuntimeException("Failed to load socialnetwork.ecore!", e);
		}
	}

}
